require 'rails_helper'

RSpec.describe Journaled::KinesisClient do
  describe '.generate' do
    context 'when JOURNALED_IAM_ROLE_ARN is defined' do
      let(:aws_sts_client) { Aws::STS::Client.new(stub_responses: true) }

      around do |example|
        with_env(JOURNALED_IAM_ROLE_ARN: 'iam-role-arn-for-assuming-kinesis-access') { example.run }
      end

      before do
        allow(Aws::AssumeRoleCredentials).to receive(:new).and_call_original

        allow(Aws::STS::Client).to receive(:new).and_return aws_sts_client
        allow(aws_sts_client).to receive(:assume_role).and_call_original
      end

      it 'initializes a Kinesis client and assumes the provided role' do
        described_class.generate

        expect(Aws::AssumeRoleCredentials).to have_received(:new).with(
          client: aws_sts_client,
          role_arn: 'iam-role-arn-for-assuming-kinesis-access',
          role_session_name: 'JournaledAssumeRoleAccess',
        )
        expect(aws_sts_client).to have_received(:assume_role).with(hash_including(role_arn: 'iam-role-arn-for-assuming-kinesis-access'))
      end
    end

    describe 'config' do
      let(:config) { described_class.generate.config }

      it 'is in us-east-1 by default' do
        with_env(AWS_DEFAULT_REGION: nil) do
          expect(config.region).to eq 'us-east-1'
        end
      end

      it 'respects AWS_DEFAULT_REGION env var' do
        with_env(AWS_DEFAULT_REGION: 'us-west-2') do
          expect(config.region).to eq 'us-west-2'
        end
      end

      it "doesn't limit retry" do
        expect(config.retry_limit).to eq 0
      end

      it 'provides no AWS credentials by default' do
        with_env(RUBY_AWS_ACCESS_KEY_ID: nil, RUBY_AWS_SECRET_ACCESS_KEY: nil) do
          expect(config.access_key_id).to eq nil
          expect(config.secret_access_key).to eq nil
        end
      end

      it 'will use legacy credentials if specified' do
        with_env(RUBY_AWS_ACCESS_KEY_ID: 'key_id', RUBY_AWS_SECRET_ACCESS_KEY: 'secret') do
          expect(config.access_key_id).to eq 'key_id'
          expect(config.secret_access_key).to eq 'secret'
        end
      end
    end
  end
end
