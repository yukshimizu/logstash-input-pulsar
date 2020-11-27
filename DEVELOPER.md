# logstash-input-pulsar

Apache Pulsar input for Logstash. This input will consume messages from Pulsar topic using the Pulsar client libraries.

For more information about Pulsar, refer to this [documentation](https://pulsar.apache.org/docs/en/standalone/).

Information about Pulsar client libraries can be found [here](https://pulsar.apache.org/docs/en/client-libraries-java/).

## Dependencies
- Apache Pulsar version 2.4.1

## Logstash Configuration
See following links for details about Pulsar client and consumer options.

- https://pulsar.apache.org/docs/en/client-libraries-java/#client
- https://pulsar.apache.org/docs/en/client-libraries-java/#consumer
- https://pulsar.apache.org/docs/en/client-libraries-java/#athenz

```sh
input {
    pulsar {
          service_url => ... # string (optional), default: "pulsar://localhost:6650", Service URL provider for Pulsar service.
          auth_plugin_class_name => ... # string (optional), Name of the authentication plugin. Default is None.
          auth_params => ... # string (optional), String represents parameters for the authentication plugin. Default is None.
          operation_timeout_ms => ... # number (optional), Operation timeout. Default is 30000.
          stats_interval_seconds => ... # number (optional), Interval between each stats info. Stats is activated with positive statsInterval. Set statsIntervalSeconds to 1 second at least. Default is 60.
          num_io_threads => ... # number (optional), The number of threads used for handling connections to brokers. Default is 1.
          num_listener_threads => ... # number (optional), The number of threads used for handling message listeners. Default is 1.
          use_tcp_no_delay => ... # boolean (optional), Whether to use TCP no-delay flag on the connection to disable Nagle algorithm. Default is true.
          use_tls => ... # boolean (optional), Whether to use TLS encryption on the connection. Default is false.
          tls_trust_certs_file_path => ... # path (optional), Path to the trusted TLS certificate file. Default is None.
          tls_allow_insecure_connection => ... # boolean (optional), Whether the Pulsar client accepts untrusted TLS certificate from broker. Default is false.
          tls_hostname_verification_enable => ... # boolean (optional), Whether to enable TLS hostname verification. Default is false.
          concurrent_lookup_request => ... # number (optional), The number of concurrent lookup requests allowed to send on each broker connection to prevent overload on broker. Default is 5000.
          max_lookup_request => ... # number (optional), The maximum number of lookup requests allowed on each broker connection to prevent overload on broker. Default is 50000.
          max_number_of_rejected_request_per_connection => ... # number (optional), he maximum number of rejected requests of a broker in a certain time frame (30 seconds) after the current connection is closed and the client creates a new connection to connect to a different broker. Default is 50.
          keep_alive_interval_seconds => ... # number (optional), Seconds of keeping alive interval for each client broker connection. Default is 30.
          connection_timeout_ms => ... # number (optional), Duration of waiting for a connection to a broker to be established. Default is 10000.
          request_timeout_ms => ... # number (optional), Maximum duration for completing a request. Default is 60000.
          default_backoff_interval_nanos => ... # number (optional), Default duration for a backoff interval. Default is TimeUnit.MILLISECONDS.toNanos(100).
          max_backoff_interval_nanos => ... # number (optional), Maximum duration for a backoff interval. Default is TimeUnit.SECONDS.toNanos(30).
          topics_name => ... # array (optional), default: ["logstash"], Topic name.
          topics_pattern => ... # string (optional), Topic pattern. Default is None.
          subscription_name => ... # string (optional), default: "logstash-group", Subscription name. Default is None.
          subscription_type => ... # string (optional), Subscription type. Three subscription types are available: Exclusive, Failover, and Shared. Plugin default is Shared.
          receive_queue_size => ... # number (optional), Size of a consumer's receiver queue. For example, the number of messages accumulated by a consumer before an application calls Receive. Default is 1000.
          acknowledgements_group_time_micros => ... # number (optional),　Group a consumer acknowledgment for a specified time. By default, a consumer uses 100ms grouping time to send out acknowledgments to a broker. Default is TimeUnit.MILLISECONDS.toMicros(100).
          negative_ack_redelivery_delay_micros => ... # number (optional), Delay to wait before redelivering messages that failed to be processed. Default is TimeUnit.MINUTES.toMicros(1).
          max_total_receive_queue_size_across_partitions => ... # number (optional), he max total receiver queue size across partitions. Default is 50000.
          consumer_name => ... # string (optional), default: "logstash-client", Consumer name.
          ack_timeout_millis => ... # number (optional), Timeout of unacked messages. Default is 0.
          tick_durations_millis => ... # number (optional), Granularity of the ack-timeout redelivery. Default is 1000.
          priority_level => ... # number (optional), Priority level for a consumer to which a broker gives more priority while dispatching messages in the shared ubscription mode. Default is 0.
          crypto_failure_action => ... # string (optional), Consumer should take action when it receives a message that can not be decrypted. Default is ConsumerCryptoFailureAction.FAIL.
          properties =>  ... # hash (optional), A name or value property of this consumer. Properties is application defined metadata attached to a consumer. Default is new TreeMap<>().
          read_compacted => ... # boolean (optional), If enabling readCompacted, a consumer reads messages from a compacted topic rather than reading a full message backlog of a topic. Default is false.
          subscription_initial_position => ... # string (optional), Initial position at which to set cursor when subscribing to a topic at first time. Default is SubscriptionInitialPosition.Latest.
          pattern_auto_discovery_period => ... # number (optional), Topic auto discovery period when using a pattern for topic's consumer. The default and minimum value is 1 minute.
          regex_subscription_mode => ... # number (optional), When subscribing to a topic using a regular expression, you can pick a certain type of topics: PersistentOnly, NonPersistentOnly, AllTopics. Default is RegexSubscriptionMode.PersistentOnly.
          auto_update_partitions => ... # boolean (optional), If autoUpdatePartitions is enabled, a consumer subscribes to partition increasement automatically. Note: this is only for partitioned consumers. Default is true.
          replicate_subscription_state => ... # boolean (optional), If replicateSubscriptionState is enabled, a subscription state is replicated to geo-replicated clusters. Default is false.
          consumer_threads => ... # number (optional), default: 1, Number of consumer threads.
          decorate_events => ... # boolean (optional), default: false, Whether the Logstash plugin adds metadata to events.
    }
}
```
Please note that you need to set **config.support_escapes to true** when you configure **auth_params** so that you can escape characters such as \n and \" in strings in the pipeline configuration files.
Refer to https://www.elastic.co/guide/en/logstash/current/configuration-file-structure.html#_escape_sequences for more details.
```
# auth_params example

input {
  pulsar {
    service_url => "pulsar+ssl://localhost:6651"
    auth_plugin_class_name => "org.apache.pulsar.client.impl.auth.AuthenticationAthenz"
    auth_params => "{\"tenantDomain\":\"shopping\",\"tenantService\":\"some_app\",\"providerDomain\":\"pulsar\",\"privateKey\":\"file:///path_to/some_app_private.pem\",\"keyId\":\"v0\",\"ztsUrl\":\"https://athenz.local:8443/zts/v1\"}"
    topics_name => ["persistent://shopping/some_app/some_topic"]
    subscription_name => "logstash-group-exclusive"
    subscription_type => "Exclusive"
    consumer_name => "logstash-client"
    consumer_threads => 1
    decorate_events => true
  }
}
```