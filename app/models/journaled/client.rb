class Journaled::Client
  DEFAULT_REGION = 'us-east-1'.freeze

  def kinesis_client
    @kinesis_client ||= Aws::Kinesis::Client.new(kinesis_client_config)
  end

  private

  def kinesis_client_config
    {
      region: ENV.fetch('AWS_DEFAULT_REGION', DEFAULT_REGION),
      retry_limit: 0,
    }.merge(credentials)
  end

  def credentials
    if ENV.key?('JOURNALED_IAM_ROLE_ARN')
      {
        credentials: iam_assume_role_credentials,
      }
    else
      legacy_credentials_hash_if_present
    end
  end

  def legacy_credentials_hash_if_present
    if ENV.key?('RUBY_AWS_ACCESS_KEY_ID')
      {
        access_key_id: ENV.fetch('RUBY_AWS_ACCESS_KEY_ID'),
        secret_access_key: ENV.fetch('RUBY_AWS_SECRET_ACCESS_KEY'),
      }
    else
      {}
    end
  end

  def sts_client
    Aws::STS::Client.new({
      region: ENV.fetch('AWS_DEFAULT_REGION', DEFAULT_REGION),
    }.merge(legacy_credentials_hash_if_present))
  end

  def iam_assume_role_credentials
    Aws::AssumeRoleCredentials.new(
      client: sts_client,
      role_arn: ENV.fetch('JOURNALED_IAM_ROLE_ARN'),
      role_session_name: "JournaledAssumeRoleAccess",
    )
  end

  class KinesisTemporaryFailure < Journaled::NotTrulyExceptionalError
  end
end
