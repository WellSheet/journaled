module Journaled::Changes
  extend ActiveSupport::Concern

  included do
    cattr_accessor(:_journaled_change_definitions) { [] }
    cattr_accessor(:journaled_attribute_names) { [] }
    cattr_accessor(:journaled_enqueue_opts, instance_writer: false) { {} }

    after_create do
      self.class._journaled_change_definitions.each do |definition|
        Journaled::ChangeWriter.new(model: self, change_definition: definition).create
      end
    end

    after_save unless: :saved_change_to_id? do
      self.class._journaled_change_definitions.each do |definition|
        Journaled::ChangeWriter.new(model: self, change_definition: definition).update
      end
    end

    after_destroy do
      self.class._journaled_change_definitions.each do |definition|
        Journaled::ChangeWriter.new(model: self, change_definition: definition).delete
      end
    end
  end

  if Rails::VERSION::MAJOR > 5 || (Rails::VERSION::MAJOR == 5 && Rails::VERSION::MINOR >= 2)
    def delete
      unless self.class.journaled_attribute_names.empty?
        raise('#delete aborted by Journaled::Changes. Call #destroy instead to ensure journaling.')
      end

      super()
    end

    def update_columns(attributes)
      unless self.class.journaled_attribute_names.empty?
        conflicting_journaled_attribute_names = self.class.journaled_attribute_names & attributes.keys.map(&:to_sym)
        raise(<<~ERROR) if conflicting_journaled_attribute_names.present?
          #update_columns aborted by Journaled::Changes due to journaled attributes:

            #{conflicting_journaled_attribute_names.join(', ')}

          Call #update instead to ensure journaling.
        ERROR
      end

      super(attributes)
    end
  end

  class_methods do
    def journal_changes_to(*attribute_names, as:, enqueue_with: {}) # rubocop:disable Naming/UncommunicativeMethodParamName
      if attribute_names.empty? || attribute_names.any? { |n| !n.is_a?(Symbol) }
        raise 'one or more symbol attribute_name arguments is required'
      end

      raise 'as: must be a symbol' unless as.is_a?(Symbol)

      _journaled_change_definitions << Journaled::ChangeDefinition.new(attribute_names: attribute_names, logical_operation: as)
      journaled_attribute_names.concat(attribute_names)
      journaled_enqueue_opts.merge!(enqueue_with)
    end

    if Rails::VERSION::MAJOR > 5 || (Rails::VERSION::MAJOR == 5 && Rails::VERSION::MINOR >= 2)
      def delete(id_or_array)
        unless journaled_attribute_names.empty?
          raise('.delete aborted by Journaled::Changes. Call .destroy(id_or_array) instead to ensure journaling.')
        end

        super(id_or_array)
      end
    end
  end
end
