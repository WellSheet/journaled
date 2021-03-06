require 'delayed_job_active_record'
rails_env = ENV['RAILS_ENV'] ||= 'test'
db_adapter = ENV['DB_ADAPTER'] ||= 'postgresql'
require File.expand_path('dummy/config/environment.rb', __dir__)

Rails.configuration.database_configuration[db_adapter][rails_env].tap do |c|
  ActiveRecord::Tasks::DatabaseTasks.create(c)
  ActiveRecord::Base.establish_connection(c)
  load File.expand_path('dummy/db/schema.rb', __dir__)
end

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.order = :random
end
