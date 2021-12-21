class Journaled::BulkWriter
  def initialize(journaled_events:, app_name:, enqueue_opts: {}, per_job_delay: 0.seconds)
    @journaled_events = journaled_events
    @app_name = app_name
    @enqueue_opts = enqueue_opts
    @per_job_delay = per_job_delay
  end

  def journal!
    serializers.each(&:serialize!)

    enqueue_deliveries!
  end

  private

  attr_reader :journaled_events, :app_name, :enqueue_opts, :per_job_delay

  def enqueue_deliveries!
    now = Time.zone.now
    serializers.each_slice(Journaled.bulk_delivery_chunk_limit).each_with_index.each do |serializers, index|
      enqueue_delivery!(serializers, now + (index * per_job_delay))
    end
  end

  def enqueue_delivery!(serializers, run_at)
    serialized_events = serializers.map(&:serialized_event)
    partition_keys = serializers.map(&:journaled_partition_key)

    Journaled.enqueue!(
      Journaled::BulkDelivery.new(
        records: serialized_events.zip(partition_keys),
        app_name: app_name,
      ),
      **enqueue_opts.merge(run_at: run_at),
    )
  end

  def serializers
    @serializers ||= journaled_events.map do |e|
      Journaled::Serializer.new(journaled_event: e)
    end
  end
end
