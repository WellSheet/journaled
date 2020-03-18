class Journaled::BulkDelivery
  def initialize(serialized_events:, partition_keys:, app_name:)
    @serialized_events = serialized_events
    @partition_keys = partition_keys
    @app_name = app_name
  end

  def perform
    return unless Journaled.enabled?

    kinesis_client.put_records request
  rescue Aws::Kinesis::Errors::InternalFailure, Aws::Kinesis::Errors::ServiceUnavailable, Aws::Kinesis::Errors::Http503Error => e
    Rails.logger.error "Kinesis Error - Server Error occurred - #{e.class}"
    raise KinesisTemporaryFailure
  rescue Seahorse::Client::NetworkingError => e
    Rails.logger.error "Kinesis Error - Networking Error occurred - #{e.class}"
    raise KinesisTemporaryFailure
  end

  private

  attr_reader :serialized_events, :partition_keys, :app_name

  def records
    serialized_events.zip(partition_keys).map do |event, partition_key|
      {
        data: event,
        partition_key: partition_key,
      }
    end
  end

  def request
    {
      stream_name: Journaled.stream_name_for_app(app_name),
      records: records,
    }
  end

  def kinesis_client
    Journaled::Client.generate
  end

  class KinesisTemporaryFailure < Journaled::NotTrulyExceptionalError
  end
end
