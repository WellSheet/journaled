class Journaled::BulkWriter
  EVENTS_PER_DELIVERY = 500

  def initialize(journaled_events:, app_name:, enqueue_opts: {})
    journaled_events.each do |event|
      Journaled::Validator.new(journaled_event: event).validate_required_methods!
    end

    @journaled_events = journaled_events.lazy
    @app_name = app_name
    @enqueue_opts = enqueue_opts
  end

  def journal!
    journaled_events.each do |event|
      Journaled::Validator.new(journaled_event: event).validate_serialized_event!
    end

    enqueue_deliveries!
  end

  private

  attr_reader :journaled_events, :app_name, :enqueue_opts

  def enqueue_deliveries!
    chunked_serialized_events.zip(chunked_partition_keys).each do |serialized_event_chunk, partition_key_chunk|
      Journaled.enqueue!(
        Journaled::BulkDelivery.new(
          serialized_events: serialized_event_chunk,
          partition_keys: partition_key_chunk,
          app_name: app_name,
        ),
        enqueue_opts,
      )
    end
  end

  def chunked_serialized_events
    serialized_events.each_slice(EVENTS_PER_DELIVERY)
  end

  def chunked_partition_keys
    partition_keys.each_slice(EVENTS_PER_DELIVERY)
  end

  def serialized_events
    journaled_events.map do |event|
      event.journaled_attributes.to_json
    end
  end

  def partition_keys
    journaled_events.map(&:journaled_partition_key)
  end
end
