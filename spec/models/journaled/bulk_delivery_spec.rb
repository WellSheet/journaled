require 'rails_helper'

RSpec.describe Journaled::BulkDelivery do
  let(:stream_name) { 'test_events' }
  let(:partition_key_1) { 'fake_partition_key' }
  let(:serialized_event_1) { '{"foo":"bar"}' }
  let(:partition_key_2) { 'different_partition_key' }
  let(:serialized_event_2) { '{"other-key":"bar"}' }
  let(:kinesis_client) { Aws::Kinesis::Client.new(stub_responses: true) }

  around do |example|
    with_env(JOURNALED_STREAM_NAME: stream_name) { example.run }
  end

  let(:serialized_events) { [serialized_event_1, serialized_event_2] }
  let(:partition_keys) { [partition_key_1, partition_key_2] }
  subject { described_class.new serialized_events: serialized_events, partition_keys: partition_keys, app_name: nil }

  describe '#perform' do
    let(:return_status_body) do
      {
        records: [
          { shard_id: '101', sequence_number: '101123' },
          { shard_id: '101', sequence_number: '101124' },
        ],
      }
    end

    before do
      kinesis_client.stub_responses(:put_records, return_status_body)
      allow(Journaled::Client).to receive(:generate).and_return kinesis_client

      allow(Journaled).to receive(:enabled?).and_return(true)
    end

    it 'makes requests to AWS to put the events on the Kinesis with the correct bodies' do
      allow(kinesis_client).to receive(:put_records).and_call_original
      response = subject.perform

      event = response.records.first
      expect(event.shard_id).to eq '101'
      expect(event.sequence_number).to eq '101123'

      expect(kinesis_client).to have_received(:put_records)
        .with(
          stream_name: 'test_events',
          records: [
            {
              data: '{"foo":"bar"}',
              partition_key: 'fake_partition_key',
            },
            {
              data: '{"other-key":"bar"}',
              partition_key: 'different_partition_key',
            },
          ],
        )
    end

    context 'when the stream name env var is NOT set' do
      let(:stream_name) { nil }

      it 'raises an KeyError error' do
        expect { subject.perform }.to raise_error KeyError
      end
    end

    context 'when Amazon responds with an InternalFailure' do
      before do
        kinesis_client.stub_responses(:put_records, 'InternalFailure')
      end

      it 'catches the error and re-raises a subclass of NotTrulyExceptionalError and logs about the failure' do
        expect(Rails.logger).to receive(:error).with("Kinesis Error - Server Error occurred - Aws::Kinesis::Errors::InternalFailure").once
        expect { subject.perform }.to raise_error described_class::KinesisTemporaryFailure
      end
    end

    context 'when Amazon responds with a ServiceUnavailable' do
      before do
        kinesis_client.stub_responses(:put_records, 'ServiceUnavailable')
      end

      it 'catches the error and re-raises a subclass of NotTrulyExceptionalError and logs about the failure' do
        allow(Rails.logger).to receive(:error)
        expect { subject.perform }.to raise_error described_class::KinesisTemporaryFailure
        expect(Rails.logger).to have_received(:error).with(/\AKinesis Error/).once
      end
    end

    context 'when we receive a 504 Gateway timeout' do
      before do
        kinesis_client.stub_responses(:put_records, 'Aws::Kinesis::Errors::ServiceError')
      end

      it 'raises an error that subclasses Aws::Kinesis::Errors::ServiceError' do
        expect { subject.perform }.to raise_error Aws::Kinesis::Errors::ServiceError
      end
    end

    context 'when the IAM user does not have permission to put_record to the specified stream' do
      before do
        kinesis_client.stub_responses(:put_records, 'AccessDeniedException')
      end

      it 'raises an AccessDeniedException error' do
        expect { subject.perform }.to raise_error Aws::Kinesis::Errors::AccessDeniedException
      end
    end

    context 'when the request timesout' do
      before do
        kinesis_client.stub_responses(:put_records, Seahorse::Client::NetworkingError.new(Timeout::Error.new))
      end

      it 'catches the error and re-raises a subclass of NotTrulyExceptionalError and logs about the failure' do
        expect(Rails.logger).to receive(:error).with(
          "Kinesis Error - Networking Error occurred - Seahorse::Client::NetworkingError",
        ).once
        expect { subject.perform }.to raise_error described_class::KinesisTemporaryFailure
      end
    end
  end
end
