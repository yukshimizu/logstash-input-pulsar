# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/pulsar"
require "rspec/wait"
require "open3"
require "socket" # for Socket.gethostname

RSpec.describe "LogStash::Inputs::Pulsar", :unit => true do

  let(:version) { "0.1.0" }
  let(:num_events) { 1 }
  let(:queue) { Queue.new }

  subject(:pulsar) { LogStash::Inputs::Pulsar.new(config) }

  describe "#register" do
    let(:config) {
      {
          "service_url" => "pulsar://localhost:6650",
          "auth_plugin_class_name" => "auth_plugin_class_name",
          "auth_params" => "auth_params",
          "operation_timeout_ms" => 30000,
          "stats_interval_seconds" => 60,
          "num_io_threads" => 1,
          "num_listener_threads" => 1,
          "use_tcp_no_delay" => true,
          "use_tls" => false,
          "tls_trust_certs_file_path" => "/etc/passwd",
          "tls_allow_insecure_connection" => false,
          "tls_hostname_verification_enable" => false,
          "concurrent_lookup_request" => 5000,
          "max_lookup_request" => 50000,
          "max_number_of_rejected_request_per_connection" => 50,
          "keep_alive_interval_seconds" => 30,
          "connection_timeout_ms" => 10000,
          "request_timeout_ms" => 60000,
          "default_backoff_interval_nanos" => 100000000,
          "max_backoff_interval_nanos" => 6000000000,
          "topics_name" => ["logstash"],
          "topics_pattern" => "persistent://public/default/finance-.*",
          "subscription_name" => "logstash-group",
          "subscription_type" => "Exclusive",
          "receive_queue_size" => 1000,
          "acknowledgements_group_time_micros" => 100000,
          "negative_ack_redelivery_delay_micros" => 60000000,
          "max_total_receive_queue_size_across_partitions" => 5000,
          "consumer_name" => "logstash-client",
          "ack_timeout_millis" => 0,
          "tick_durations_millis" => 1000,
          "priority_level" => 0,
          "crypto_failure_action" => "FAIL",
          "properties" => { "key" => "value"},
          "read_compacted" => false,
          "subscription_initial_position" => "Latest",
          "pattern_auto_discovery_period" => 1,
          "regex_subscription_mode" => "PersistentOnly",
          "auto_update_partitions" => true,
          "replicate_subscription_state" => false,
          "consumer_threads" => 1,
          "decorate_events" => false
      }
    }

    it "registers" do
      expect { pulsar.register }.to_not raise_error
    end

    after do
      pulsar.stop
    end
  end

  context  "creates pulsar client with valid configuration" do
    let(:config) {
      {
          "service_url" => "pulsar://localhost:6650",
          #"auth_plugin_class_name" => "auth_plugin_class_name",
          "auth_params" => "auth_params",
          "operation_timeout_ms" => 30000,
          "stats_interval_seconds" => 60,
          "num_io_threads" => 1,
          "num_listener_threads" => 1,
          "use_tcp_no_delay" => true,
          "use_tls" => false,
          "tls_trust_certs_file_path" => "/etc/passwd",
          "tls_allow_insecure_connection" => false,
          "tls_hostname_verification_enable" => false,
          "concurrent_lookup_request" => 5000,
          "max_lookup_request" => 50000,
          "max_number_of_rejected_request_per_connection" => 50,
          "keep_alive_interval_seconds" => 30,
          "connection_timeout_ms" => 10000,
          "request_timeout_ms" => 60000,
          "default_backoff_interval_nanos" => 100000000,
          "max_backoff_interval_nanos" => 6000000000
      }
    }

    it "successfully configure client" do

      consumer_mock = double("Pulsar consumer")
      allow(consumer_mock).to receive(:close)

      consumer_builder_mock = double("Pulsar consumer builder")
      allow(consumer_builder_mock).to receive(:subscribe).and_return(consumer_mock)

      allow(pulsar).to receive(:create_consumer).and_return(consumer_builder_mock)
      allow(pulsar).to receive(:stop?).and_return(true)

      pulsar.register

      expect { pulsar.run(queue) }.to_not raise_error
      expect(queue.length).to eq(0)
    end

    after do
      pulsar.stop
    end
  end

  context "creates pulsar client with invalid configuration" do
    let(:config) {
      {
          "auth_plugin_class_name" => "auth_plugin_class_name",
          "auth_params" => "auth_params",
      }
    }

    it "raises an error when client configuration failed" do
      pulsar.register

      expect { pulsar.run(queue) }.to raise_error(org.apache.pulsar.client.api.PulsarClientException)
      expect(queue.length).to eq(0)
    end

    after do
      pulsar.stop
    end
  end

  context "creates pulsar consumer builder with valid configuration" do
    let(:config) {
      {
          "topics_name" => ["logstash"],
          "topics_pattern" => "persistent://public/default/finance-.*",
          "subscription_name" => "logstash-group",
          "subscription_type" => "Exclusive",
          "receive_queue_size" => 1000,
          "acknowledgements_group_time_micros" => 100000,
          "negative_ack_redelivery_delay_micros" => 60000000,
          "max_total_receive_queue_size_across_partitions" => 5000,
          "consumer_name" => "logstash-client",
          "ack_timeout_millis" => 0,
          "tick_durations_millis" => 1000,
          "priority_level" => 0,
          "crypto_failure_action" => "FAIL",
          "properties" => { "key" => "value"},
          "read_compacted" => false,
          "subscription_initial_position" => "Latest",
          "pattern_auto_discovery_period" => 1,
          "regex_subscription_mode" => "PersistentOnly",
          "auto_update_partitions" => true,
          "replicate_subscription_state" => false,
          "consumer_threads" => 1,
          "decorate_events" => false
      }
    }

    it "successfully configure consumer builder" do

      consumer_thread_mock = double("Pulsar consumer thread")
      allow(consumer_thread_mock).to receive(:join)
      allow(consumer_thread_mock).to receive(:exit)

      allow(pulsar).to receive(:thread_runner).and_return(consumer_thread_mock)

      pulsar.register

      expect { pulsar.run(queue) }.to_not raise_error
      expect(queue.length).to eq(0)
    end

    after do
      pulsar.stop
    end
  end

  context "creates pulsar consumer builder with invalid configuration" do
    let(:config) {
      {
          "topics_pattern" => "\\invalid_pattern"
      }
    }

    it "raises an error when consumer configuration failed" do
      pulsar.register

      expect { pulsar.run(queue) }.to raise_error(java.util.regex.PatternSyntaxException)
      expect(queue.length).to eq(0)
    end

    after do
      pulsar.stop
    end
  end

  describe "receives message without event decoration" do
    let(:config) {
      {
          "decorate_events" => false
      }
    }

    it "acknowledges after getting successfully received a message" do
      host = Socket.gethostname
      message_mock = double("pulsar message")
      allow(message_mock).to receive(:getValue.to_s).and_return("Hello-Pulsar")

      consumer_mock = double("pulsar consumer")
      allow(consumer_mock).to receive(:receive).and_return(message_mock)
      allow(consumer_mock).to receive(:acknowledge)
      #allow(consumer_mock).to receive(:negativeAcknowledge)
      allow(consumer_mock).to receive(:close)

      consumer_builder_mock = double("Pulsar consumer builder")
      allow(consumer_builder_mock).to receive(:loadConf).and_return(consumer_builder_mock)
      allow(consumer_builder_mock).to receive(:subscribe).and_return(consumer_mock)

      client_builder_mock = double("Pulsar client builder")
      allow(client_builder_mock).to receive(:loadConf)
      allow(client_builder_mock).to receive(:newConsumer).and_return(consumer_builder_mock)
      allow(client_builder_mock).to receive(:close)

      allow(pulsar).to receive(:create_client).and_return(client_builder_mock)
      allow(pulsar).to receive(:stop?).and_return(false, true)

      pulsar.register

      expect { pulsar.run(queue) }.to_not raise_error
      expect(queue.length).to eq(num_events)
      event = queue.pop
      expect(event.get("host")).to eq(host)
      expect(event.get("[@metadata][pulsar][version]")).to eq(version)
    end

    it "negative acknowledges when receiving an unexpected message" do
      consumer_mock = double("pulsar consumer")
      allow(consumer_mock).to receive(:receive).and_return("Invalid-Message")
      allow(consumer_mock).to receive(:negativeAcknowledge)
      allow(consumer_mock).to receive(:close)

      consumer_builder_mock = double("Pulsar consumer builder")
      allow(consumer_builder_mock).to receive(:loadConf).and_return(consumer_builder_mock)
      allow(consumer_builder_mock).to receive(:subscribe).and_return(consumer_mock)

      client_builder_mock = double("Pulsar client builder")
      allow(client_builder_mock).to receive(:loadConf)
      allow(client_builder_mock).to receive(:newConsumer).and_return(consumer_builder_mock)
      allow(client_builder_mock).to receive(:close)

      allow(pulsar).to receive(:create_client).and_return(client_builder_mock)
      allow(pulsar).to receive(:stop?).and_return(false, true)

      pulsar.register

      expect { pulsar.run(queue) }.to_not raise_error
      expect(queue.length).to eq(0)
    end

    after do
      pulsar.stop
    end

  end

  describe "receives message with event decoration" do
    let(:config) {
      {
          "decorate_events" => true
      }
    }

    def event_chk(event)
      if event.get("[@metadata][pulsar][topic]") == "logstash" &&
          event.get("[@metadata][pulsar][producer]") == "producer" &&
          event.get("[@metadata][pulsar][key]") == "key" &&
          event.get("[@metadata][pulsar][timestamp]") == 1601354923
        true
      else
        false
      end
    end

    it "acknowledges after getting successfully received a message" do
      message_mock = double("pulsar message")
      allow(message_mock).to receive(:getValue.to_s).and_return("Hello-Pulsar")
      allow(message_mock).to receive(:getTopicName).and_return("logstash")
      allow(message_mock).to receive(:getProducerName).and_return("producer")
      allow(message_mock).to receive(:getKey).and_return("key")
      allow(message_mock).to receive(:getEventTime).and_return(1601354923)
      allow(message_mock).to receive(:hasKey).and_return(true)

      consumer_mock = double("pulsar consumer")
      allow(consumer_mock).to receive(:receive).and_return(message_mock)
      allow(consumer_mock).to receive(:acknowledge)
      allow(consumer_mock).to receive(:close)

      consumer_builder_mock = double("Pulsar consumer builder")
      allow(consumer_builder_mock).to receive(:loadConf).and_return(consumer_builder_mock)
      allow(consumer_builder_mock).to receive(:subscribe).and_return(consumer_mock)

      client_builder_mock = double("Pulsar client builder")
      allow(client_builder_mock).to receive(:loadConf)
      allow(client_builder_mock).to receive(:newConsumer).and_return(consumer_builder_mock)
      allow(client_builder_mock).to receive(:close)

      allow(pulsar).to receive(:create_client).and_return(client_builder_mock)
      allow(pulsar).to receive(:stop?).and_return(false, true)

      pulsar.register

      expect { pulsar.run(queue) }.to_not raise_error
      expect(queue.length).to eq(num_events)
      event = queue.pop
      expect(event_chk(event)).to eql(true)
    end

    after do
      pulsar.stop
    end
  end

end
