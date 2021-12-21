class Journaled::Writer
  def initialize(journaled_event:)
    @journaled_event = journaled_event
  end

  def journal!
    serializer.serialize!

    Journaled.enqueue!(journaled_delivery, **journaled_enqueue_opts)
  end

  private

  attr_reader :journaled_event
  delegate :journaled_partition_key, :journaled_app_name, :journaled_enqueue_opts, to: :journaled_event
  delegate :serialized_event, to: :serializer

  def journaled_delivery
    Journaled::Delivery.new(
      serialized_event: serialized_event,
      partition_key: journaled_partition_key,
      app_name: journaled_app_name,
    )
  end

  def serializer
    @serializer ||= Journaled::Serializer.new(journaled_event: journaled_event)
  end
end
