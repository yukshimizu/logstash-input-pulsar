# encoding: utf-8
require "logstash/namespace"
require "logstash/inputs/base"
require "stud/interval"
require "socket" # for Socket.gethostname
require "java"
require "logstash-input-pulsar_jars"

# Generate a repeating message.

class LogStash::Inputs::Pulsar < LogStash::Inputs::Base
  config_name "pulsar"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # Pulsar client configuration
  #
  # Service URL provider for Pulsar service. Default is None.
  config :service_url, :validate => :string, :default => "pulsar://localhost:6650"
  # Proxy service URL. Default is None.
  config :proxy_service_url, :validate => :string
  # Proxy protocol. e.g. SNI. Default is None.
  config :proxy_protocol, :validate => :string
  # Name of the authentication plugin. Default is None.
  config :auth_plugin_class_name, :validate => :string
  # String represents parameters for the authentication plugin. Example: key1:val1,key2:val2. Default is None.
  config :auth_params, :validate => :string
  # Operation timeout. Default is 30000.
  config :operation_timeout_ms, :validate => :number
  # Interval between each stats info. Stats is activated with positive statsInterval.
  # Set statsIntervalSeconds to 1 second at least. Default is 60.
  config :stats_interval_seconds, :validate => :number
  # The number of threads used for handling connections to brokers. Default is 1.
  config :num_io_threads, :validate => :number
  # The number of threads used for handling message listeners. Default is 1.
  config :num_listener_threads, :validate => :number
  # Whether to use TCP no-delay flag on the connection to disable Nagle algorithm. Default is true.
  config :use_tcp_no_delay, :validate => :boolean
  # Whether to use TLS encryption on the connection. Default is false.
  config :use_tls, :validate => :boolean
  # Path to the trusted TLS certificate file. Default is None.
  config :tls_trust_certs_file_path, :validate => :path
  # Whether the Pulsar client accepts untrusted TLS certificate from broker. Default is false.
  config :tls_allow_insecure_connection, :validate => :boolean
  # Whether to enable TLS hostname verification. Default is false.
  config :tls_hostname_verification_enable, :validate => :boolean
  # The number of concurrent lookup requests allowed to send on each broker connection to prevent overload on broker.
  # Default is 5000.
  config :concurrent_lookup_request, :validate => :number
  # The maximum number of lookup requests allowed on each broker connection to prevent overload on broker.
  # Default is 50000.
  config :max_lookup_request, :validate => :number
  # The maximum number of rejected requests of a broker in a certain time frame (30 seconds)
  # after the current connection is closed and the client creates a new connection to connect to a different broker.
  # Default is 50.
  config :max_number_of_rejected_request_per_connection, :validate => :number
  # Seconds of keeping alive interval for each client broker connection. Default is 30.
  config :keep_alive_interval_seconds, :validate => :number
  # Duration of waiting for a connection to a broker to be established.
  # If the duration passes without a response from a broker, the connection attempt is dropped. Default is 10000.
  config :connection_timeout_ms, :validate => :number
  # Maximum duration for completing a request. Default is 60000.
  config :request_timeout_ms, :validate => :number
  # Default duration for a backoff interval. Default is TimeUnit.MILLISECONDS.toNanos(100).
  # CAUTION! This parameter's name is "initialBackoffIntervalNanos" actually, Pulsar document is wrong.
  config :default_backoff_interval_nanos, :validate => :number
  # Maximum duration for a backoff interval. Default is TimeUnit.SECONDS.toNanos(30).
  config :max_backoff_interval_nanos, :validate => :number

  # Pulsar consumer configuration
  #
  # Topic name.
  config :topics_name, :validate => :array, :default => ["logstash"]
  # Topic pattern. Default is None.
  config :topics_pattern, :validate => :string
  # Subscription name. Default is None.
  config :subscription_name, :validate => :string, :default => "logstash-group"
  # Subscription type. Three subscription types are available: Exclusive, Failover, and Shared. Default is Exclusive.
  # But, the plugin's default is Shared because Exclusive does not make sense if default number of thread is 1.
  config :subscription_type, :validate => :string
  # Size of a consumer's receiver queue. For example, the number of messages accumulated by a consumer
  # before an application calls Receive. A value higher than the default value increases consumer throughput,
  # though at the expense of more memory utilization. Default is 1000.
  config :receive_queue_size, :validate => :number
  # Group a consumer acknowledgment for a specified time. By default, a consumer uses 100ms grouping time to send out
  # acknowledgments to a broker. Setting a group time of 0 sends out acknowledgments immediately. A longer ack group
  # time is more efficient at the expense of a slight increase in message re-deliveries after a failure.
  # Default is TimeUnit.MILLISECONDS.toMicros(100).
  config :acknowledgements_group_time_micros, :validate => :number
  # Delay to wait before redelivering messages that failed to be processed. When an application uses
  # {@link Consumer#negativeAcknowledge(Message)}, failed messages are redelivered after a fixed timeout.
  # Default is TimeUnit.MINUTES.toMicros(1).
  config :negative_ack_redelivery_delay_micros, :validate => :number
  # The max total receiver queue size across partitions. This setting reduces the receiver queue size for
  # individual partitions if the total receiver queue size exceeds this value. Default is 50000.
  config :max_total_receive_queue_size_across_partitions, :validate => :number
  # Consumer name. Default is null.
  config :consumer_name, :validate => :string, :default => "logstash-client"
  # Timeout of unacked messages. Default is 0.
  config :ack_timeout_millis, :validate => :number
  # Granularity of the ack-timeout redelivery. Using an higher tickDurationMillis reduces the memory overhead
  # to track messages when setting ack-timeout to a bigger value (for example, 1 hour). Default is 1000.
  config :tick_durations_millis, :validate => :number
  # Priority level for a consumer to which a broker gives more priority while dispatching messages in the shared
  # subscription mode. The broker follows descending priorities. For example, 0=max-priority, 1, 2,...
  # In shared subscription mode, the broker first dispatches messages to the max priority level consumers
  # if they have permits. Otherwise, the broker considers next priority level consumers. Default is 0.
  #
  # Example 1
  # If a subscription has consumerA with priorityLevel 0 and consumerB with priorityLevel 1, then the broker only
  # dispatches messages to consumerA until it runs out permits and then starts dispatching messages to consumerB.
  #
  # Example2
  # Consumer Priority, Level, Permits
  # C1, 0, 2
  # C2, 0, 1
  # C3, 0, 1
  # C4, 1, 2
  # C5, 1, 1
  # Order in which a broker dispatches messages to consumers is: C1, C2, C3, C1, C4, C5, C4.
  config :priority_level, :validate => :number
  # Consumer should take action when it receives a message that can not be decrypted.
  # FAIL: this is the default option to fail messages until crypto succeeds.
  # DISCARD: silently acknowledge and not deliver message to an application.
  # CONSUME: deliver encrypted messages to applications. It is the application's responsibility to decrypt the message.
  # The decompression of message fails. If messages contain batch messages, a client is not be able to retrieve
  # individual messages in batch.
  # Delivered encrypted message contains {@link EncryptionContext} which contains encryption and compression
  # information in it using which application can decrypt consumed message payload.
  # Default is ConsumerCryptoFailureAction.FAIL.
  config :crypto_failure_action, :validate => :string
  # A name or value property of this consumer. Properties is application defined metadata attached to a consumer.
  # When getting a topic stats, associate this metadata with the consumer stats for easier identification.
  # Default is new TreeMap<>().
  config :properties, :validate => :hash
  # If enabling readCompacted, a consumer reads messages from a compacted topic rather than reading a full message
  # backlog of a topic. A consumer only sees the latest value for each key in the compacted topic, up until reaching
  # the point in the topic message when compacting backlog. Beyond that point, send messages as normal.
  # Only enabling readCompacted on subscriptions to persistent topics, which have a single active consumer
  # (like failure or exclusive subscriptions). Attempting to enable it on subscriptions to non-persistent topics or
  # on shared subscriptions leads to a subscription call throwing a PulsarClientException. Default is false.
  config :read_compacted, :validate => :boolean
  # Initial position at which to set cursor when subscribing to a topic at first time.
  # Default is SubscriptionInitialPosition.Latest.
  config :subscription_initial_position, :validate => :string
  # Topic auto discovery period when using a pattern for topic's consumer. The default and minimum value is 1 minute.
  config :pattern_auto_discovery_period, :validate => :number
  # When subscribing to a topic using a regular expression, you can pick a certain type of topics.
  # PersistentOnly: only subscribe to persistent topics.
  # NonPersistentOnly: only subscribe to non-persistent topics.
  # AllTopics: subscribe to both persistent and non-persistent topics.
  # Default is RegexSubscriptionMode.PersistentOnly.
  config :regex_subscription_mode, :validate => :string
  # Dead letter policy for consumers.
  # By default, some messages are probably redelivered many times, even to the extent that it never stops.
  # By using the dead letter mechanism, messages have the max redelivery count. When exceeding the maximum number
  # of redeliveries, messages are sent to the Dead Letter Topic and acknowledged automatically.
  # You can enable the dead letter mechanism by setting deadLetterPolicy. Default is None.
  #
  # Example
  #
  # client.newConsumer()
  # .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10).build())
  # .subscribe();
  #
  # Default dead letter topic name is {TopicName}-{Subscription}-DLQ.
  #
  # To set a custom dead letter topic name:
  # client.newConsumer()
  # .deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(10)
  # .deadLetterTopic("your-topic-name").build())
  # .subscribe();
  #
  # When specifying the dead letter policy while not specifying ackTimeoutMillis, you can set the ack timeout to
  # 30000 millisecond.
  # This is not implemented in this plugin.
  #config :dead_letter_policy
  # If autoUpdatePartitions is enabled, a consumer subscribes to partition increasement automatically.
  # Note: this is only for partitioned consumers. Default is true.
  config :auto_update_partitions, :validate => :boolean
  # If replicateSubscriptionState is enabled, a subscription state is replicated to geo-replicated clusters.
  # Default is false.
  config :replicate_subscription_state, :validate => :boolean

  config :consumer_threads, :validate => :number, :default => 1
  config :decorate_events, :validate => :boolean, :default => false

  PLUGIN_VERSION = "0.2.0"


  public
  def register
    logger.info("Registering logstash-input-pulsar")
    @host = Socket.gethostname
    @version = PLUGIN_VERSION
    @runner_threads = []
  end # def register

  def run(logstash_queue)
    @runner_client = create_client
    @runner_consumers = consumer_threads.times.map { |i| create_consumer(@runner_client,"#{@consumer_name}-#{i}") }
    @runner_threads = @runner_consumers.map { |consumer_builder| thread_runner(logstash_queue, consumer_builder) }
    @runner_threads.each { |t| t.join }
  end # def run

  public
  def stop
    logger.info("Stopping all pulsar client !!!")
    @runner_client && @runner_client.close
    @runner_threads.each { |t| t.exit }
  end

  public
  def pulsar_consumers
    @runner_consumers
  end

  private
  def create_client
    begin
      logger.info("Creating pulsar client")

      # Configure Pulsar Client loadconf
      client_config = java.util.HashMap.new
      client_config.put("serviceUrl", @service_url)
      client_config.put("proxyServiceUrl", @proxy_service_url) unless @proxy_service_url.nil?
      client_config.put("proxyProtocol", @proxy_protocol) unless @proxy_protocol.nil?
      client_config.put("authPluginClassName", @auth_plugin_class_name) unless @auth_plugin_class_name.nil?
      client_config.put("authParams", @auth_params) unless @auth_params.nil?
      client_config.put("operationTimeoutMs", @operation_timeout_ms) unless @operation_timeout_ms.nil?
      client_config.put("statsIntervalSeconds", @stats_interval_seconds) unless @stats_interval_seconds.nil?
      client_config.put("numIoThreads", @num_io_threads) unless @num_io_threads.nil?
      client_config.put("numListenerThreads", @num_listener_threads) unless @num_listener_threads.nil?
      client_config.put("useTcpNoDelay", @use_tcp_no_delay) unless @use_tcp_no_delay.nil?
      client_config.put("useTls", @use_tls) unless @use_tls.nil?
      client_config.put("tlsTrustCertsFilePath", @tls_trust_certs_file_path) unless @tls_trust_certs_file_path.nil?
      client_config.put("tlsAllowInsecureConnection", @tls_allow_insecure_connection) unless @tls_allow_insecure_connection.nil?
      client_config.put("tlsHostnameVerificationEnable", @tls_hostname_verification_enable) unless @tls_hostname_verification_enable.nil?
      client_config.put("concurrentLookupRequest", @concurrent_lookup_request) unless @concurrent_lookup_request.nil?
      client_config.put("maxLookupRequest", @max_lookup_request) unless @max_lookup_request.nil?
      client_config.put("maxNumberOfRejectedRequestPerConnection", @max_number_of_rejected_request_per_connection) unless @max_number_of_rejected_request_per_connection.nil?
      client_config.put("keepAliveIntervalSeconds", @keep_alive_interval_seconds) unless @keep_alive_interval_seconds.nil?
      client_config.put("connectionTimeoutMs", @connection_timeout_ms) unless @connection_timeout_ms.nil?
      client_config.put("requestTimeoutMs", @request_timeout_ms) unless @request_timeout_ms.nil?
      client_config.put("initialBackoffIntervalNanos", @default_backoff_interval_nanos) unless @default_backoff_interval_nanos.nil?
      client_config.put("maxBackoffIntervalNanos", @max_backoff_interval_nanos) unless @max_backoff_interval_nanos.nil?

      client_builder = org.apache.pulsar.client.api.PulsarClient.builder
      client_builder.loadConf(client_config)
      client_builder.build
    rescue => e
      logger.error("Unable to create pulsar client from given configuration",
                   :pulsar_error_message => e,
                   :cause => e.respond_to?(:getCause) ? e.getCause() : nil)
      raise e
    end
  end

  private
  def create_consumer(client, consumer_name)
    begin
      logger.info("client - ", :client => consumer_name)

      # Configure Pulsar Consumer loadconf
      consumer_config = java.util.HashMap.new

      unless @topics_pattern.nil?
        pattern = java.util.regex.Pattern.compile(@topics_pattern)
        consumer_config.put("topicsPattern", pattern)
        logger.info("topic:",:topic => @topics_pattern)
      else
        consumer_config.put("topicNames", @topics_name)
        logger.info("topic:",:topic => @topics_name)
      end

      consumer_config.put("subscriptionName", @subscription_name)

      enum_subscription_type = org.apache.pulsar.client.api.SubscriptionType
      if @subscription_type == "Exclusive"
        consumer_config.put("subscriptionType", enum_subscription_type::Exclusive)
      elsif @subscription_type == "Failover"
        consumer_config.put("subscriptionType", enum_subscription_type::Failover)
      else
        consumer_config.put("subscriptionType", enum_subscription_type::Shared)
      end

      consumer_config.put("receiverQueueSize", @receive_queue_size) unless @receive_queue_size.nil?
      consumer_config.put("acknowledgementsGroupTimeMicros", @acknowledgements_group_time_micros) unless @acknowledgements_group_time_micros.nil?
      consumer_config.put("negativeAckRedeliveryDelayMicros", @negative_ack_redelivery_delay_micros) unless @negative_ack_redelivery_delay_micros.nil?
      consumer_config.put("maxTotalReceiverQueueSizeAcrossPartitions", @max_total_receive_queue_size_across_partitions) unless @max_total_receive_queue_size_across_partitions.nil?
      consumer_config.put("consumerName", consumer_name)
      consumer_config.put("ackTimeoutMillis", @ack_timeout_millis) unless @ack_timeout_millis.nil?
      consumer_config.put("tickDurationMillis", @tick_durations_millis) unless @tick_durations_millis.nil?
      consumer_config.put("priorityLevel", @priority_level) unless @priority_level.nil?

      unless @crypto_failure_action.nil?
        enum_crypto_failure_action = org.apache.pulsar.client.api.ConsumerCryptoFailureAction
        if @crypto_failure_action == "DISCARD"
          consumer_config.put("cryptoFailureAction", enum_crypto_failure_action::DISCARD)
        elsif @crypto_failure_action == "CONSUME"
          consumer_config.put("cryptoFailureAction", enum_crypto_failure_action::CONSUME)
        else
          consumer_config.put("cryptoFailureAction", enum_crypto_failure_action::FAIL)
        end
      end

      consumer_config.put("properties", @properties) unless @properties.nil?
      consumer_config.put("readCompacted", @read_compacted) unless @read_compacted.nil?

      unless @subscription_initial_position.nil?
        enum_subscription_initial_position = org.apache.pulsar.client.api.SubscriptionInitialPosition
        if @subscription_initial_position == "Ealiest"
          consumer_config.put("subscriptionInitialPosition", enum_subscription_initial_position::Ealiest)
        else
          consumer_config.put("subscriptionInitialPosition", enum_subscription_initial_position::Latest)
        end
      end

      consumer_config.put("patternAutoDiscoveryPeriod", @pattern_auto_discovery_period) unless @pattern_auto_discovery_period.nil?

      unless @regex_subscription_mode.nil?
        enum_regex_subscription_mode = org.apache.pulsar.client.api.RegexSubscriptionMode
        if @regex_subscription_mode == "NonPersistentOnly"
          consumer_config.put("regexSubscriptionMode", enum_regex_subscription_mode::NonPersistentOnly)
        elsif @regex_subscription_mode == "AllTopics"
          consumer_config.put("regexSubscriptionMode", enum_regex_subscription_mode::AllTopics)
        else
          consumer_config.put("regexSubscriptionMode", enum_regex_subscription_mode::PersistentOnly)
        end
      end

      #consumer_config.put("deadLetterPolicy") #NOT implemented
      consumer_config.put("autoUpdatePartitions", @auto_update_partitions) unless @auto_update_partitions.nil?
      consumer_config.put("replicateSubscriptionState", @replicate_subscription_state) unless @replicate_subscription_state.nil?

      consumer_builder = client.newConsumer
      consumer_builder.loadConf(consumer_config)
    rescue => e
      logger.error("Unable to create pulsar consumer from given configuration",
                   :pulsar_error_message => e,
                   :cause => e.respond_to?(:getCause) ? e.getCause() : nil)
      raise e
    end
  end

  private
  def thread_runner(logstash_queue, consumer_builder)
    Thread.new do
      begin
        consumer = consumer_builder.subscribe
        codec_instance = @codec.clone

        while !stop?
          record = consumer.receive

          begin
            codec_instance.decode(record.getValue.to_s) do |event|
              decorate(event)
              event.set("host", @host)
              event.set("[@metadata][pulsar][version]", @version)
              if @decorate_events
                event.set("[@metadata][pulsar][topic]", record.getTopicName)
                event.set("[@metadata][pulsar][producer]", record.getProducerName)
                event.set("[@metadata][pulsar][key]", record.getKey) if record.hasKey
                event.set("[@metadata][pulsar][timestamp]", record.getPublishTime)
              end
              logstash_queue << event
              consumer.acknowledge(record)
            end
          rescue => e
            logger.warn("Failed to process message !",
                         :pulsar_error_message => e,
                         :cause => e.respond_to?(:getCause) ? e.getCause() : nil)
            consumer.negativeAcknowledge(record)
          end
        end
      rescue => e
        logger.error("exit - ",:cause => e.respond_to?(:getCause) ? e.getCause() : nil)
        raise e if !stop?
      ensure
        logger.info("Closing pulsar consumer !")
        consumer.close
      end
    end
  end

end # class LogStash::Inputs::Pulsar
