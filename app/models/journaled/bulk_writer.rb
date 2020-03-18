class Journaled::BulkWriter
  EVENTS_PER_DELIVERY = 500

  def initialize(journaled_events:, app_name:, enqueue_opts: {})
    @journaled_events = journaled_events.lazy
    @app_name = app_name
    @enqueue_opts = enqueue_opts
  end

  def journal!
    serializers.each(&:serialize!)

    enqueue_deliveries!
  end

  private

  attr_reader :journaled_events, :app_name, :enqueue_opts

  def enqueue_deliveries!
    serializers.each_slice(EVENTS_PER_DELIVERY).each do |serializers|
      enqueue_delivery!(serializers)
    end
  end

  def enqueue_delivery!(serializers)
    serialized_events = serializers.map(&:serialized_event)
    partition_keys = serializers.map(&:journaled_partition_key)

    Journaled.enqueue!(
      Journaled::BulkDelivery.new(
        records: serialized_events.zip(partition_keys),
        app_name: app_name,
      ),
      enqueue_opts,
    )
  end

  def serializers
    @serializers ||= journaled_events.map do |e|
      Journaled::Serializer.new(journaled_event: e)
    end
  end
end
