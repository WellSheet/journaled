class Journaled::Serializer
  REQUIRED_EVENT_METHOD_NAMES = %i(
    journaled_schema_name
    journaled_partition_key
    journaled_attributes
    journaled_app_name
    journaled_enqueue_opts
  ).freeze

  attr_reader :serialized_event
  delegate :journaled_partition_key, to: :journaled_event

  def initialize(journaled_event:)
    @journaled_event = journaled_event
  end

  def serialize!
    validate_required_methods!

    serialized_event = journaled_attributes.to_json
    validate_serialized_event!(serialized_event)

    @serialized_event = serialized_event
  end

  private

  attr_reader :journaled_event
  delegate(*REQUIRED_EVENT_METHOD_NAMES, to: :journaled_event)

  def validate_required_methods!
    unless respond_to_all?(journaled_event, REQUIRED_EVENT_METHOD_NAMES)
      raise "An enqueued event must respond to: #{REQUIRED_EVENT_METHOD_NAMES.to_sentence}"
    end

    unless journaled_event.journaled_schema_name.present? &&
        journaled_event.journaled_partition_key.present? &&
        journaled_event.journaled_attributes.present?
      raise <<~ERROR
        An enqueued event must have a non-nil response to:
          #json_schema_name,
          #partition_key, and
          #journaled_attributes
      ERROR
    end
  end

  def validate_serialized_event!(serialized_event)
    base_event_json_schema_validator.validate! serialized_event
    json_schema_validator.validate! serialized_event
  end

  def json_schema_validator
    @json_schema_validator ||= Journaled::JsonSchemaModel::Validator.new(journaled_schema_name)
  end

  def base_event_json_schema_validator
    @base_event_json_schema_validator ||= Journaled::JsonSchemaModel::Validator.new('base_event')
  end

  def respond_to_all?(object, method_names)
    method_names.all? do |method_name|
      object.respond_to?(method_name)
    end
  end
end
