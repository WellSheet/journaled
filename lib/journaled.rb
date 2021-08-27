require 'aws-sdk-kinesis'
require 'delayed_job'
require 'json-schema'
require 'request_store'

require 'journaled/engine'
require 'journaled/enqueue'
require 'journaled/stream_name.rb'

module Journaled
  mattr_accessor :default_app_name
  mattr_accessor(:job_priority) { 20 }
  mattr_accessor :job_queue

  def development_or_test?
    %w(development test).include?(Rails.env)
  end

  def enabled?
    !['0', 'false', false, 'f', ''].include?(ENV.fetch('JOURNALED_ENABLED', !development_or_test?))
  end

  def bulk_delivery_limit
    ENV.fetch('JOURNALED_BULK_DELIVERY_LIMIT', 500)
  end

  def schema_providers
    @schema_providers ||= [Journaled::Engine, Rails]
  end

  def commit_hash
    ENV.fetch('GIT_COMMIT')
  end

  def actor_uri
    Journaled::ActorUriProvider.instance.actor_uri
  end

  module_function :development_or_test?, :enabled?, :bulk_delivery_limit, :schema_providers, :commit_hash, :actor_uri
end
