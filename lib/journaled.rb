require 'aws-sdk-kinesis'
require 'delayed'
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

  def bulk_delivery_chunk_limit
    ENV.fetch('JOURNALED_BULK_DELIVERY_CHUNK_LIMIT', 500).to_i
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

  module_function :development_or_test?, :enabled?, :schema_providers, :commit_hash, :actor_uri,
                  :bulk_delivery_chunk_limit
end
