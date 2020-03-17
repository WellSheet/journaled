class Journaled::BulkWriter
  def initialize(journaled_events:, app_name:)
    journaled_events.each do |event|
      Journaled::Validator.new(journaled_event: event).validate_required_methods!
    end

    @journaled_events = journaled_events
    @app_name = app_name
  end

  def journal!
    journaled_events.each do |event|
      Journaled::Validator.new(journaled_event: event).validate_serialized_event!
    end

    Journaled.enqueue!(journaled_delivery, {})
  end

  private

  attr_reader :journaled_events, :app_name

  def journaled_delivery
    Journaled::BulkDelivery.new(
      serialized_events: serialized_events,
      partition_keys: journaled_events.map(&:journaled_partition_key),
      app_name: app_name,
    )
  end

  def serialized_events
    journaled_events.map do |event|
      event.journaled_attributes.to_json
    end
  end
end
