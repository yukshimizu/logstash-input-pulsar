# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/pulsar"
require "rspec/wait"
require "open3"

# Please run pulsar as standalone mode prior to executing this integration test.
RSpec.describe "LogStash::Inputs::Pulsar", :integration => true do

  let(:timeout_seconds) { 30 }
  let(:num_events) { 10 }
  let(:service_url) { "pulsar+ssl://localhost:6651" } #If testing TLS-less environment, set "pulsar://localhost:6650"

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
      Open3.capture3("pulsar-admin topics create persistent://public/default/logstashE")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://public/default/logstashE")
      Open3.capture3("pulsar-client produce logstashE -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_name" => ["logstashE"],
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/logstashE -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/logstashE")
    end

  end

  context "for single topic with Shared mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://public/default/logstashS")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://public/default/logstashS")
      Open3.capture3("pulsar-client produce logstashS -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_name" => ["logstashS"],
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/logstashS -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/logstashS")
    end

  end

  context "for single topic with Failover mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://public/default/logstashF")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://public/default/logstashF")
      Open3.capture3("pulsar-client produce logstashF -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_name" => ["logstashF"],
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/logstashF -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/logstashF")
    end
  end

  context "for multi-topics with Exclusive mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://public/default/multiE-1")
      Open3.capture3("pulsar-admin topics create persistent://public/default/multiE-2")
      Open3.capture3("pulsar-admin topics create persistent://public/default/multiE-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://public/default/multiE-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://public/default/multiE-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://public/default/multiE-3")
      Open3.capture3("pulsar-client produce multiE-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce multiE-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce multiE-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_name" => ["multiE-1", "multiE-2", "multiE-3"],
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/multiE-1 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/multiE-2 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/multiE-3 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/multiE-1")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/multiE-2")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/multiE-3")
    end

  end

  context "for multi-topics with Shared mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://public/default/multiS-1")
      Open3.capture3("pulsar-admin topics create persistent://public/default/multiS-2")
      Open3.capture3("pulsar-admin topics create persistent://public/default/multiS-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://public/default/multiS-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://public/default/multiS-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://public/default/multiS-3")
      Open3.capture3("pulsar-client produce multiS-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce multiS-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce multiS-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_name" => ["multiS-1", "multiS-2", "multiS-3"],
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/multiS-1 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/multiS-2 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/multiS-3 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/multiS-1")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/multiS-2")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/multiS-3")
    end

  end

  context "for multi-topics with Failover mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://public/default/multiF-1")
      Open3.capture3("pulsar-admin topics create persistent://public/default/multiF-2")
      Open3.capture3("pulsar-admin topics create persistent://public/default/multiF-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://public/default/multiF-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://public/default/multiF-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://public/default/multiF-3")
      Open3.capture3("pulsar-client produce multiF-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce multiF-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce multiF-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_name" => ["multiF-1", "multiF-2", "multiF-3"],
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/multiF-1 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/multiF-2 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/multiF-3 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/multiF-1")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/multiF-2")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/multiF-3")
    end

  end

  context "for topic-pattern with Exclusive mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://public/default/patternE-1")
      Open3.capture3("pulsar-admin topics create persistent://public/default/patternE-2")
      Open3.capture3("pulsar-admin topics create persistent://public/default/patternE-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://public/default/patternE-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://public/default/patternE-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://public/default/patternE-3")
      Open3.capture3("pulsar-client produce patternE-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce patternE-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce patternE-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_pattern" => "persistent://public/default/patternE-.*",
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/patternE-1 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/patternE-2 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/patternE-3 -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/patternE-1")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/patternE-2")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/patternE-3")
    end

  end

  context "for topic-pattern with Shared mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://public/default/patternS-1")
      Open3.capture3("pulsar-admin topics create persistent://public/default/patternS-2")
      Open3.capture3("pulsar-admin topics create persistent://public/default/patternS-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://public/default/patternS-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://public/default/patternS-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://public/default/patternS-3")
      Open3.capture3("pulsar-client produce patternS-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce patternS-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce patternS-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_pattern" => "persistent://public/default/patternS-.*",
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/patternS-1 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/patternS-2 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/patternS-3 -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/patternS-1")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/patternS-2")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/patternS-3")
    end

  end

  context "for topic-pattern with Failover mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://public/default/patternF-1")
      Open3.capture3("pulsar-admin topics create persistent://public/default/patternF-2")
      Open3.capture3("pulsar-admin topics create persistent://public/default/patternF-3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://public/default/patternF-1")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://public/default/patternF-2")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://public/default/patternF-3")
      Open3.capture3("pulsar-client produce patternF-1 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce patternF-2 -m \"Hello-Pulsar\" -n 10")
      Open3.capture3("pulsar-client produce patternF-3 -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_pattern" => "persistent://public/default/patternF-.*",
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/patternF-1 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/patternF-2 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/patternF-3 -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/patternF-1")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/patternF-2")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/patternF-3")
    end

  end

  context "for partitioned-topic with Exclusive mode" do
    before do
      Open3.capture3("pulsar-admin topics create-partitioned-topic persistent://public/default/partitionedE -p 3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://public/default/partitionedE")
      Open3.capture3("pulsar-client produce partitionedE -m \"Hello-Pulsar\" -n 30")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_pattern" => "persistent://public/default/partitionedE",
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/partitionedE -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics delete-partitioned-topic persistent://public/default/partitionedE")
    end

  end

  context "for partitioned-topic with Shared mode" do
    before do
      Open3.capture3("pulsar-admin topics create-partitioned-topic persistent://public/default/partitionedS -p 3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-shared persistent://public/default/partitionedS")
      Open3.capture3("pulsar-client produce partitionedS -m \"Hello-Pulsar\" -n 30")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_pattern" => "persistent://public/default/partitionedS",
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/partitionedS -s logstash-group-shared -f")
      Open3.capture3("pulsar-admin topics delete-partitioned-topic persistent://public/default/partitionedS")
    end

  end

  context "for partitioned-topic with Failover mode" do
    before do
      Open3.capture3("pulsar-admin topics create-partitioned-topic persistent://public/default/partitionedF -p 3")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-failover persistent://public/default/partitionedF")
      Open3.capture3("pulsar-client produce partitionedF -m \"Hello-Pulsar\" -n 30")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_pattern" => "persistent://public/default/partitionedF",
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
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/partitionedF -s logstash-group-failover -f")
      Open3.capture3("pulsar-admin topics delete-partitioned-topic persistent://public/default/partitionedF")
    end

  end

  context "when decorated for single topic with Exclusive mode" do
    before do
      Open3.capture3("pulsar-admin topics create persistent://public/default/logstashDE")
      Open3.capture3("pulsar-admin topics create-subscription -s logstash-group-exclusive persistent://public/default/logstashDE")
      Open3.capture3("pulsar-client produce logstashDE -m \"Hello-Pulsar\" -n 10")
      pulsar.register
    end

    let(:config) {
      {
          "service_url" => service_url,
          "topics_name" => ["logstashDE"],
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
        expect(event.get("[@metadata][pulsar][topic]")).to eq("persistent://public/default/logstashDE")
        expect(event.get("[@metadata][pulsar][producer]")).to start_with("standalone")
        expect(event.get("[@metadata][pulsar][key]")).to be_falsey
        expect(event.get("[@metadata][pulsar][timestamp]")).to to be > 0
      ensure
        t.kill
        t.join(30_000)
      end
    end

    after do
      pulsar.stop
      Open3.capture3("pulsar-admin topics unsubscribe persistent://public/default/logstashDE -s logstash-group-exclusive -f")
      Open3.capture3("pulsar-admin topics delete persistent://public/default/logstashDE")
    end

  end

end
