module Journaled::RelationChangeProtection
  def update_all(updates) # rubocop:disable Metrics/MethodLength, Metrics/AbcSize, Metrics/PerceivedComplexity
    if @klass.respond_to?(:journaled_attribute_names) && !@klass.journaled_attribute_names.empty?
      conflicting_journaled_attribute_names = if updates.is_a?(Hash)
                                                @klass.journaled_attribute_names & updates.keys.map(&:to_sym)
                                              elsif updates.is_a?(String)
                                                @klass.journaled_attribute_names.select do |a|
                                                  updates.match?(/\b(?<!')#{a}(?!')\b/)
                                                end
                                              else
                                                raise "unsupported type '#{updates.class}' for 'updates'"
                                              end
      raise(<<~ERROR) if conflicting_journaled_attribute_names.present?
        .update_all aborted by Journaled::Changes due to journaled attributes:

          #{conflicting_journaled_attribute_names.join(', ')}

        Consider using .all(lock: true) or .find_each with #update to ensure journaling.
      ERROR
    end

    super(updates)
  end

  def delete_all
    if @klass.respond_to?(:journaled_attribute_names) && !@klass.journaled_attribute_names.empty?
      raise('#delete_all aborted by Journaled::Changes. Call .destroy_all instead to ensure journaling.')
    end

    super()
  end
end
