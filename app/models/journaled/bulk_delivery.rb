class Journaled::BulkDelivery
  def initialize(records:, app_name:)
    @records = records
    @app_name = app_name
  end

  def perform
    return unless Journaled.enabled?

    response = kinesis_client.put_records request
    reenqueue_failed_records!(response)
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

  def reenqueue_failed_records!(response) # rubocop:disable Metrics/AbcSize
    records_with_responses = records.zip(response.records)
    errored_records = records_with_responses.select { |_record, resp| resp.error_code.present? }.map(&:first)

    failed_record_count = response.failed_record_count || 0
    raise 'FailedRecordCount differs from count of records that have errors' unless errored_records.count == failed_record_count
    raise 'ALL Records failed to be added to the Kinesis steam' if errored_records.count == records.count

    Delayed::Job.enqueue self.class.new(records: errored_records, app_name: app_name)
  end

  class KinesisTemporaryFailure < Journaled::NotTrulyExceptionalError
  end
end
