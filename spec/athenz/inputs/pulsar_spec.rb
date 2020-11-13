# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/pulsar"
require "rspec/wait"
require "open3"

# Please run pulsar as standalone mode prior to executing this integration test.
RSpec.describe "LogStash::Inputs::Pulsar", :athenz => true do

  let(:timeout_seconds) { 30 }
  let(:num_events) { 10 }
  let(:service_url) { "pulsar+ssl://localhost:6651"} #If testing TLS-less environment, set "pulsar://localhost:6650"
  let(:auth_plugin_class_name) { "org.apache.pulsar.client.impl.auth.AuthenticationAthenz" }
  let(:auth_params) { "{\"tenantDomain\":\"shopping\",\"tenantService\":\"some_app\",\"providerDomain\":\"pulsar\",\"privateKey\":\"file:///path_to/private.pem\",\"keyId\":\"v0\",\"ztsUrl\":\"https://athenz.local:8443/zts/v1\"}" }

  subject(:pulsar) { LogStash::Inputs::Pulsar.new(config) }

  def thread_it(pulsar_input, queue)
    Thread.new do
      begin
        pulsar_input.run(queue)
      end
    end
  end

  context "for single topic with Exclusive mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/logstashE")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://shopping/some_app/logstashE")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/logstashE -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_name" => ["persistent://shopping/some_app/logstashE"],
          "subscription_name" => "logstash-group-exclusive",
          "subscription_type" => "Exclusive",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 1  #NOTE: This setting must be 1, when subscription_type is Exclusive.
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events)
        expect(queue.length).to eq(num_events)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/logstashE -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/logstashE")
    end

  end

  context "for single topic with Shared mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/logstashS")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://shopping/some_app/logstashS")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/logstashS -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_name" => ["persistent://shopping/some_app/logstashS"],
          "subscription_name" => "logstash-group-shared",
          "subscription_type" => "Shared",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 2
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events)
        expect(queue.length).to eq(num_events)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/logstashS -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/logstashS")
    end

  end

  context "for single topic with Failover mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/logstashF")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://shopping/some_app/logstashF")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/logstashF -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_name" => ["persistent://shopping/some_app/logstashF"],
          "subscription_name" => "logstash-group-failover",
          "subscription_type" => "Failover",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 2
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events)
        expect(queue.length).to eq(num_events)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/logstashF -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/logstashF")
    end
  end

  context "for multi-topics with Exclusive mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/multiE-1")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/multiE-2")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/multiE-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://shopping/some_app/multiE-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://shopping/some_app/multiE-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://shopping/some_app/multiE-3")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/multiE-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/multiE-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/multiE-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_name" => ["persistent://shopping/some_app/multiE-1", "persistent://shopping/some_app/multiE-2", "persistent://shopping/some_app/multiE-3"],
          "subscription_name" => "logstash-group-exclusive",
          "subscription_type" => "Exclusive",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 1  #NOTE: This setting must be 1, when subscription_type is Exclusive.
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events*3)
        expect(queue.length).to eq(num_events*3)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/multiE-1 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/multiE-2 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/multiE-3 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/multiE-1")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/multiE-2")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/multiE-3")
    end

  end

  context "for multi-topics with Shared mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/multiS-1")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/multiS-2")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/multiS-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://shopping/some_app/multiS-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://shopping/some_app/multiS-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://shopping/some_app/multiS-3")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/multiS-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/multiS-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/multiS-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_name" => ["persistent://shopping/some_app/multiS-1", "persistent://shopping/some_app/multiS-2", "persistent://shopping/some_app/multiS-3"],
          "subscription_name" => "logstash-group-shared",
          "subscription_type" => "Shared",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 2
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events*3)
        expect(queue.length).to eq(num_events*3)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/multiS-1 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/multiS-2 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/multiS-3 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/multiS-1")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/multiS-2")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/multiS-3")
    end

  end

  context "for multi-topics with Failover mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/multiF-1")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/multiF-2")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/multiF-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://shopping/some_app/multiF-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://shopping/some_app/multiF-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://shopping/some_app/multiF-3")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/multiF-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/multiF-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/multiF-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_name" => ["persistent://shopping/some_app/multiF-1", "persistent://shopping/some_app/multiF-2", "persistent://shopping/some_app/multiF-3"],
          "subscription_name" => "logstash-group-failover",
          "subscription_type" => "Failover",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 2
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events*3)
        expect(queue.length).to eq(num_events*3)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/multiF-1 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/multiF-2 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/multiF-3 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/multiF-1")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/multiF-2")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/multiF-3")
    end

  end

  context "for topic-pattern with Exclusive mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/patternE-1")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/patternE-2")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/patternE-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://shopping/some_app/patternE-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://shopping/some_app/patternE-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://shopping/some_app/patternE-3")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/patternE-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/patternE-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/patternE-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_pattern" => "persistent://shopping/some_app/patternE-.*",
          "subscription_name" => "logstash-group-exclusive",
          "subscription_type" => "Exclusive",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 1  #NOTE: This setting must be 1, when subscription_type is Exclusive.
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events*3)
        expect(queue.length).to eq(num_events*3)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/patternE-1 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/patternE-2 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/patternE-3 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics persistent://shopping/some_app/delete persistent://shopping/some_app/patternE-1")
      Open3.capture3("pulsar-admin topics persistent://shopping/some_app/delete persistent://shopping/some_app/patternE-2")
      Open3.capture3("pulsar-admin topics persistent://shopping/some_app/delete persistent://shopping/some_app/patternE-3")
    end

  end

  context "for topic-pattern with Shared mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/patternS-1")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/patternS-2")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/patternS-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://shopping/some_app/patternS-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://shopping/some_app/patternS-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://shopping/some_app/patternS-3")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/patternS-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/patternS-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/patternS-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_pattern" => "persistent://shopping/some_app/patternS-.*",
          "subscription_name" => "logstash-group-shared",
          "subscription_type" => "Shared",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 2
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events*3)
        expect(queue.length).to eq(num_events*3)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/patternS-1 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/patternS-2 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/patternS-3 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/patternS-1")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/patternS-2")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/patternS-3")
    end

  end

  context "for topic-pattern with Failover mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/patternF-1")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/patternF-2")
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/patternF-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://shopping/some_app/patternF-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://shopping/some_app/patternF-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://shopping/some_app/patternF-3")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/patternF-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/patternF-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/patternF-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_pattern" => "persistent://shopping/some_app/patternF-.*",
          "subscription_name" => "logstash-group-failover",
          "subscription_type" => "Failover",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 2
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events*3)
        expect(queue.length).to eq(num_events*3)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/patternF-1 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/patternF-2 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/patternF-3 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/patternF-1")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/patternF-2")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/patternF-3")
    end

  end

  context "for partitioned-topic with Exclusive mode" do
    before do
      Open3.capture3("pulsar-admin topics create-partitioned-topic persistent://shopping/some_app/partitionedE -p 3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://shopping/some_app/partitionedE")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/partitionedE -m \"Hello-Pulsar\" -n 30")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_pattern" => "persistent://shopping/some_app/partitionedE",
          "subscription_name" => "logstash-group-exclusive",
          "subscription_type" => "Exclusive",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 1  #NOTE: This setting must be 1, when subscription_type is Exclusive.
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events*3)
        expect(queue.length).to eq(num_events*3)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/partitionedE -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics delete-partitioned-topic persistent://shopping/some_app/partitionedE")
    end

  end

  context "for partitioned-topic with Shared mode" do
    before do
      Open3.capture3("pulsar-admin topics create-partitioned-topic persistent://shopping/some_app/partitionedS -p 3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://shopping/some_app/partitionedS")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/partitionedS -m \"Hello-Pulsar\" -n 30")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_pattern" => "persistent://shopping/some_app/partitionedS",
          "subscription_name" => "logstash-group-shared",
          "subscription_type" => "Shared",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 2
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events*3)
        expect(queue.length).to eq(num_events*3)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/partitionedS -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics delete-partitioned-topic persistent://shopping/some_app/partitionedS")
    end

  end

  context "for partitioned-topic with Failover mode" do
    before do
      Open3.capture3("pulsar-admin topics create-partitioned-topic persistent://shopping/some_app/partitionedF -p 3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://shopping/some_app/partitionedF")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/partitionedF -m \"Hello-Pulsar\" -n 30")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_pattern" => "persistent://shopping/some_app/partitionedF",
          "subscription_name" => "logstash-group-failover",
          "subscription_type" => "Failover",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 2
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events*3)
        expect(queue.length).to eq(num_events*3)
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/partitionedF -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics delete-partitioned-topic persistent://shopping/some_app/partitionedF")
    end

  end

  context "when decorated for single topic with Exclusive mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://shopping/some_app/logstashDE")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://shopping/some_app/logstashDE")
      Open3.capture3("pulsar-client produce persistent://shopping/some_app/logstashDE -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "auth_plugin_class_name" => auth_plugin_class_name,
          "auth_params" => auth_params,
          "topics_name" => ["persistent://shopping/some_app/logstashDE"],
          "subscription_name" => "logstash-group-exclusive",
          "subscription_type" => "Exclusive",
          "consumer_name" => "logstash-client",
          "consumer_threads" => 1,  #NOTE: This setting must be 1, when subscription_type is Exclusive.
          "decorate_events" => true
      }
    }
    let(:queue) { Queue.new }

    it "should consume all messages" do
      t = thread_it(pulsar, queue)
      begin
        t.run
        wait(timeout_seconds).for {queue.length}.to eq(num_events)
        expect(queue.length).to eq(num_events)
        event = queue.pop
        expect(event.get("[@metadata][pulsar][topic]")).to eq("persistent://shopping/some_app/logstashDE")
        expect(event.get("[@metadata][pulsar][producer]")).to start_with("standalone")
        expect(event.get("[@metadata][pulsar][key]")).to be_falsey
        expect(event.get("[@metadata][pulsar][timestamp]")).to be > 0
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://shopping/some_app/logstashDE -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics delete persistent://shopping/some_app/logstashDE")
    end

  end

end
