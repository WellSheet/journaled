require 'rails_helper'

RSpec.describe Journaled::Serializer do
  let(:schema_path) { Journaled::Engine.root.join "journaled_schemas/fake_schema_name.json" }
  let(:schema_file_contents) do
    <<-JSON
          {
            "title": "Foo",
            "type": "object",
            "properties": {
              "foo": {
                "type": "string"
              }
            },
            "required": ["foo"]
          }
    JSON
  end

  before do
    allow(File).to receive(:exist?).and_call_original
    allow(File).to receive(:exist?).with(schema_path).and_return(true)
    allow(File).to receive(:read).and_call_original
    allow(File).to receive(:read).with(schema_path).and_return(schema_file_contents)
  end

  let(:journaled_enqueue_opts) { {} }
  let(:journaled_event) do
    double(
      journaled_schema_name: :fake_schema_name,
      journaled_attributes: journaled_event_attributes,
      journaled_partition_key: 'fake_partition_key',
      journaled_app_name: 'my_app',
      journaled_enqueue_opts: journaled_enqueue_opts,
    )
  end

  around do |example|
    with_jobs_delayed { example.run }
  end

  subject { described_class.new journaled_event: journaled_event }

  describe '#serialize!' do
    context 'when the Journaled Event does not implement all the necessary methods' do
      let(:journaled_event) { double }

      it 'raises' do
        expect { subject.serialize! }.to raise_error RuntimeError, /An enqueued event must respond to/
      end
    end

    context 'when the Journaled Event returns non-present values for some of the required methods' do
      let(:journaled_event) do
        double(
          journaled_schema_name: nil,
          journaled_attributes: {},
          journaled_partition_key: '',
          journaled_app_name: nil,
          journaled_enqueue_opts: {},
        )
      end

      it 'raises' do
        expect { subject.serialize! }.to raise_error RuntimeError, /An enqueued event must have a non-nil response to/
      end
    end

    context 'when the journaled event does NOT comply with the base_event schema' do
      let(:journaled_event_attributes) { { foo: 1 } }

      it 'raises an error' do
        expect { subject.serialize! }.to raise_error JSON::Schema::ValidationError
      end
    end

    context 'when the event complies with the base_event schema' do
      context 'when the specific json schema is NOT valid' do
        let(:journaled_event_attributes) { { id: 'FAKE_UUID', event_type: 'fake_event', created_at: Time.zone.now, foo: 1 } }

        it 'raises an error' do
          expect { subject.serialize! }.to raise_error JSON::Schema::ValidationError
        end
      end

      context 'when the specific json schema is also valid' do
        let(:journaled_event_attributes) {
          { id: 'FAKE_UUID', event_type: 'fake_event', created_at: Time.zone.parse('2020-03-18T17:55:00Z'), foo: :bar }
        }

        it 'serialized the event correctly' do
          expect { subject.serialize! }
            .to change { subject.serialized_event }
            .from(nil)
            .to('{"id":"FAKE_UUID","event_type":"fake_event","created_at":"2020-03-18T17:55:00.000Z","foo":"bar"}')
        end
      end
    end
  end
end
