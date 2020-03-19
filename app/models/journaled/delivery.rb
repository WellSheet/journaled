class Journaled::Delivery
  def initialize(serialized_event:, partition_key:, app_name:)
    @serialized_event = serialized_event
    @partition_key = partition_key
    @app_name = app_name
  end

  def perform
    kinesis_client.put_record record if Journaled.enabled?
  rescue Aws::Kinesis::Errors::InternalFailure, Aws::Kinesis::Errors::ServiceUnavailable, Aws::Kinesis::Errors::Http503Error => e
    Rails.logger.error "Kinesis Error - Server Error occurred - #{e.class}"
    raise KinesisTemporaryFailure
  rescue Seahorse::Client::NetworkingError => e
    Rails.logger.error "Kinesis Error - Networking Error occurred - #{e.class}"
    raise KinesisTemporaryFailure
  end

  private

  attr_reader :serialized_event, :partition_key, :app_name

  def record
    {
      stream_name: Journaled.stream_name_for_app(app_name),
      data: serialized_event,
      partition_key: partition_key,
    }
  end

  def kinesis_client
    Journaled::KinesisClient.generate
  end

  class KinesisTemporaryFailure < Journaled::NotTrulyExceptionalError
  end
end
