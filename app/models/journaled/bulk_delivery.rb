class Journaled::BulkDelivery
  def initialize(serialized_events:, partition_key:, app_name:)
    @serialized_events = serialized_events
    @partition_key = partition_key
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

  def stream_name
    env_var_name = [app_name&.upcase, 'JOURNALED_STREAM_NAME'].compact.join('_')
    ENV.fetch(env_var_name)
  end

  private

  attr_reader :serialized_events, :partition_key, :app_name

  def records
    serialized_events.map do |event|
      {
        data: event,
        partition_key: partition_key,
      }
    end
  end

  def request
    {
      stream_name: stream_name,
      records: records,
    }
  end

  def kinesis_client
    Journaled::Client.generate
  end

  class KinesisTemporaryFailure < Journaled::NotTrulyExceptionalError
  end
end
