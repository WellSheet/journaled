require 'rails_helper'

RSpec.describe Journaled::Writer do
  subject { described_class.new journaled_event: journaled_event }

  describe '#journal!' do
    before do
      allow(Journaled::Serializer).to receive(:new).with(journaled_event: journaled_event).and_return(stub_serializer)
    end

    let(:journaled_enqueue_opts) { {} }
    let(:journaled_event) do
      double(
        journaled_schema_name: :fake_schema_name,
        journaled_attributes: {},
        journaled_partition_key: 'fake_partition_key',
        journaled_app_name: 'my_app',
        journaled_enqueue_opts: journaled_enqueue_opts,
      )
    end

    around do |example|
      with_jobs_delayed { example.run }
    end

    context 'when the journaled event does NOT serializer correctly' do
      let(:stub_serializer) { instance_double(Journaled::Serializer, serialize!: true, serialized_event: nil) }
      before do
        allow(stub_serializer).to receive(:serialize!).and_raise('An enqueued event must respond to: journaled_attributes')
      end

      it 'raises an error and does not enqueue anything' do
        expect { subject.journal! }.to raise_error(/An enqueued event must respond to: journaled_attributes/)
        expect(Delayed::Job.where('handler like ?', '%Journaled::Delivery%').count).to eq 0
      end
    end

    context 'when the journaled event can be serialized' do
      let(:stub_serializer) { instance_double(Journaled::Serializer, serialize!: true, serialized_event: 'FAKE_SERIALIZED_EVENT') }

      it 'creates a delivery with the app name passed through' do
        allow(Journaled::Delivery).to receive(:new).and_call_original

        expect { subject.journal! }.to change {
          Delayed::Job.where('handler like ?', '%Journaled::Delivery%').count
        }.from(0).to(1)

        expect(Journaled::Delivery)
          .to have_received(:new)
          .with(app_name: 'my_app', partition_key: 'fake_partition_key', serialized_event: 'FAKE_SERIALIZED_EVENT')
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
            Delayed::Job.where('handler like ?', '%Journaled::Delivery%').where(priority: 999).count
          }.from(0).to(1)
        end
      end

      context 'when there is a job priority specified in the enqueue opts' do
        let(:journaled_enqueue_opts) { { priority: 13 } }

        it 'enqueues a Journaled::Delivery object with the given priority' do
          expect { subject.journal! }.to change {
            Delayed::Job.where('handler like ?', '%Journaled::Delivery%').where(priority: 13).count
          }.from(0).to(1)
        end
      end
    end
  end
end
