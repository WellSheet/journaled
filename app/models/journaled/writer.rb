class Journaled::Writer
  def initialize(journaled_event:)
    @journaled_event = journaled_event

    validator.validate_required_methods!
  end

  def journal!
    validator.validate_serialized_event!

    Journaled.enqueue!(journaled_delivery, journaled_enqueue_opts)
  end

  private

  attr_reader :journaled_event
  delegate :journaled_attributes, :journaled_partition_key, :journaled_app_name, :journaled_enqueue_opts, to: :journaled_event

  def journaled_delivery
    Journaled::Delivery.new(
      serialized_event: journaled_attributes.to_json,
      partition_key: journaled_partition_key,
      app_name: journaled_app_name,
    )
  end

  def validator
    @validator ||= Journaled::Validator.new(journaled_event: journaled_event)
  end
end
