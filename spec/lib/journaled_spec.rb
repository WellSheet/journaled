require 'rails_helper'

RSpec.describe Journaled do
  it "is enabled in production" do
    allow(Rails).to receive(:env).and_return("production")
    expect(described_class).to be_enabled
  end

  it "is disabled in development" do
    allow(Rails).to receive(:env).and_return("development")
    expect(described_class).not_to be_enabled
  end

  it "is disabled in test" do
    allow(Rails).to receive(:env).and_return("test")
    expect(described_class).not_to be_enabled
  end

  it "is enabled in whatevs" do
    allow(Rails).to receive(:env).and_return("whatevs")
    expect(described_class).to be_enabled
  end

  it "is enabled when explicitly enabled in development" do
    with_env(JOURNALED_ENABLED: true) do
      allow(Rails).to receive(:env).and_return("development")
      expect(described_class).to be_enabled
    end
  end

  it "is disabled when explicitly disabled in production" do
    with_env(JOURNALED_ENABLED: false) do
      allow(Rails).to receive(:env).and_return("production")
      expect(described_class).not_to be_enabled
    end
  end

  it "is disabled when explicitly disabled with empty string" do
    with_env(JOURNALED_ENABLED: '') do
      allow(Rails).to receive(:env).and_return("production")
      expect(described_class).not_to be_enabled
    end
  end

  describe "#actor_uri" do
    it "delegates to ActorUriProvider" do
      allow(Journaled::ActorUriProvider).to receive(:instance)
        .and_return(instance_double(Journaled::ActorUriProvider, actor_uri: "my actor uri"))
      expect(described_class.actor_uri).to eq "my actor uri"
    end
  end

  describe '.stream_name_for_app' do
    before do
      allow(ENV).to receive(:fetch).and_return("expected_stream_name")
    end

    it "is fetched from a prefixed ENV var if specified when app_name is unspecified" do
      expect(described_class.stream_name_for_app(nil)).to eq("expected_stream_name")
      expect(ENV).to have_received(:fetch).with("JOURNALED_STREAM_NAME")
    end

    it "is fetched from a prefixed ENV var if specified when app_name is specified" do
      expect(described_class.stream_name_for_app("my_funky_app_name")).to eq("expected_stream_name")
      expect(ENV).to have_received(:fetch).with("MY_FUNKY_APP_NAME_JOURNALED_STREAM_NAME")
    end
  end
end
