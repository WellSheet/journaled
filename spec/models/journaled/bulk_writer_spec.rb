require 'rails_helper'

RSpec.describe Journaled::BulkWriter do
  describe '#journal!' do
    let(:enqueue_opts) { {} }

    let(:journaled_event_1) do
      double
    end
    let(:journaled_event_2) do
      double
    end

    let(:journaled_events) { [journaled_event_1, journaled_event_2] }
    subject { described_class.new journaled_events: journaled_events, app_name: 'my_app', enqueue_opts: enqueue_opts }

    let(:stub_serializer_1) do
      instance_double(
        Journaled::Serializer,
        serialize!: true,
        serialized_event: 'FAKE_SERIALIZED_EVENT_1',
        journaled_partition_key: 'key_1',
      )
    end
    let(:stub_serializer_2) do
      instance_double(
        Journaled::Serializer,
        serialize!: true,
        serialized_event: 'FAKE_SERIALIZED_EVENT_2',
        journaled_partition_key: 'key_2',
      )
    end
    before do
      allow(Journaled::Serializer).to receive(:new).with(journaled_event: journaled_event_1).and_return(stub_serializer_1)
      allow(Journaled::Serializer).to receive(:new).with(journaled_event: journaled_event_2).and_return(stub_serializer_2)
    end

    around do |example|
      with_jobs_delayed { example.run }
    end

    context 'when one of the events does not serialize correctly' do
      let(:stub_serializer_1) { instance_double(Journaled::Serializer, serialize!: true, serialized_event: nil) }
      before do
        allow(stub_serializer_1).to receive(:serialize!).and_raise('An enqueued event must respond to: journaled_attributes')
      end

      it 'raises and does not enqueue anything' do
        expect { subject.journal! }.to raise_error RuntimeError, /An enqueued event must respond to/
        expect(Delayed::Job.count).to eq 0
      end
    end

    context 'when all the events are serializable' do
      it 'enqueues a delivery with the correct params' do
        allow(Journaled::BulkDelivery).to receive(:new).and_call_original
        expect { subject.journal! }.to change {
          Delayed::Job.where('handler like ?', '%Journaled::BulkDelivery%').count
        }.from(0).to(1)

        expect(Journaled::BulkDelivery).to have_received(:new)
          .with(
            app_name: 'my_app',
            records: [%w(FAKE_SERIALIZED_EVENT_1 key_1), %w(FAKE_SERIALIZED_EVENT_2 key_2)],
        )
      end

      context 'when the number of events passed in exceeds EVENTS_PER_DELIVERY' do
        before do
          stub_const("#{described_class}::EVENTS_PER_DELIVERY", 1)
        end

        it 'creates multiple deliveries' do
          allow(Journaled::BulkDelivery).to receive(:new).and_call_original
          expect { subject.journal! }.to change {
            Delayed::Job.where('handler like ?', '%Journaled::BulkDelivery%').count
          }.from(0).to(2)

          expect(Journaled::BulkDelivery).to have_received(:new)
            .with(hash_including(app_name: 'my_app')).twice
          expect(Journaled::BulkDelivery).to have_received(:new)
            .with(app_name: 'my_app', records: [%w(FAKE_SERIALIZED_EVENT_1 key_1)]).once
          expect(Journaled::BulkDelivery).to have_received(:new)
            .with(app_name: 'my_app', records: [%w(FAKE_SERIALIZED_EVENT_2 key_2)]).once
        end
      end

      context 'when there is no job priority specified in the enqueue opts' do
        around do |example|
          old_priority = Journaled.job_priority
          Journaled.job_priority = 999
          example.run
          Journaled.job_priority = old_priority
        end

        it 'defaults to the global default' do
          expect { subject.journal! }.to change {
            Delayed::Job.where('handler like ?', '%Journaled::BulkDelivery%').where(priority: 999).count
          }.from(0).to(1)
        end
      end

      context 'when there is a job priority specified in the enqueue opts' do
        let(:enqueue_opts) { { priority: 13 } }

        it 'enqueues a Journaled::Delivery object with the given priority' do
          expect { subject.journal! }.to change {
            Delayed::Job.where('handler like ?', '%Journaled::BulkDelivery%').where(priority: 13).count
          }.from(0).to(1)
        end
      end
    end
  end
end
