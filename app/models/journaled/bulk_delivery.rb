class Journaled::BulkDelivery
  def initialize(records:, app_name:)
    @records = records
    @app_name = app_name
  end

  def perform
    return unless Journaled.enabled?

    response = kinesis_client.put_records request
    handle_failures!(response)
  rescue Aws::Kinesis::Errors::InternalFailure, Aws::Kinesis::Errors::ServiceUnavailable, Aws::Kinesis::Errors::Http503Error => e
    Rails.logger.error "Kinesis Error - Server Error occurred - #{e.class}"
    raise KinesisTemporaryFailure
  rescue Seahorse::Client::NetworkingError => e
    Rails.logger.error "Kinesis Error - Networking Error occurred - #{e.class}"
    raise KinesisTemporaryFailure
  end

  private

  attr_reader :records, :app_name

  def request
    {
      stream_name: Journaled.stream_name_for_app(app_name),
      records: kinesis_records,
    }
  end

  def kinesis_records
    records.map do |event, partition_key|
      {
        data: event,
        partition_key: partition_key,
      }
    end
  end

  def kinesis_client
    Journaled::KinesisClient.generate
  end

  def handle_failures!(response)
    ErrorHandler.new(response, records, app_name).handle!
  end

  class ErrorHandler
    attr_reader :response, :records, :app_name

    def initialize(response, records, app_name)
      @response = response
      @records = records
      @app_name = app_name
    end

    def handle!
      handle_failed_record_count_mismatch!
      handle_all_records_failed!
      renenqueue_failed_records!
    end

    private

    def handle_failed_record_count_mismatch!
      failed_record_count = response.failed_record_count || 0

      unless errored_records_with_responses.count == failed_record_count
        raise 'FailedRecordCount differs from count of records that have errors'
      end
    end

    def handle_all_records_failed!
      return unless errored_records.count == records.count

      if error_objects.count == 1
        raise(error_objects.first)
      else
        raise MultipleErrorsFailure, error_objects
      end
    end

    def renenqueue_failed_records!
      Delayed::Job.enqueue Journaled::BulkDelivery.new(records: errored_records, app_name: app_name) if errored_records.any?
    end

    def error_objects
      @error_objects ||= begin
                           grouped_errors = errors.group_by(&:error_code).to_a

                           grouped_errors.map do |error_code, r|
                             error_class_for_code(error_code).new(r.map(&:error_message).join("\n"))
                           end
                         end
    end

    def error_class_for_code(error_code)
      if error_code == 'ProvisionedThroughputExceededException'
        KinesisBulkRateLimitFailure
      else
        KinesisBulkInternalErrorFailure
      end
    end

    def errored_records
      errored_records_with_responses.map(&:first)
    end

    def errors
      errored_records_with_responses.map(&:second)
    end

    def errored_records_with_responses
      @errored_records_with_responses ||= records_with_responses.select { |_record, resp| resp.error_code.present? }
    end

    def records_with_responses
      @records_with_responses ||= records.zip(response.records)
    end
  end

  class KinesisTemporaryFailure < Journaled::NotTrulyExceptionalError
  end
  class KinesisBulkInternalErrorFailure < Journaled::NotTrulyExceptionalError
  end
  class KinesisBulkRateLimitFailure < StandardError
  end

  class MultipleErrorsFailure < StandardError
    def initialize(errors)
      msgs = errors.map { |e| "#{e.class}\n#{e.message}" }
      super(msgs.join("\n\n"))
    end
  end
end
