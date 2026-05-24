namespace Kuestenlogik.Surgewave.Connector.Pulsar;

/// <summary>
/// Configuration constants for Apache Pulsar connector.
/// </summary>
public static class PulsarConnectorConfig
{
    // Connection
    public const string ServiceUrl = "pulsar.service.url";
    public const string Topic = "topic";
    public const string Topics = "pulsar.topics";
    public const string TopicsPattern = "pulsar.topics.pattern";
    public const string Subscription = "pulsar.subscription";
    public const string SubscriptionType = "pulsar.subscription.type";

    // Authentication
    public const string AuthPluginClassName = "pulsar.auth.plugin.class.name";
    public const string AuthParams = "pulsar.auth.params";
    public const string TlsTrustCertsFilePath = "pulsar.tls.trust.certs.file.path";
    public const string TlsAllowInsecureConnection = "pulsar.tls.allow.insecure.connection";

    // Consumer settings
    public const string ConsumerName = "pulsar.consumer.name";
    public const string AckTimeoutMs = "pulsar.ack.timeout.ms";
    public const string NegativeAckRedeliveryDelayMs = "pulsar.negative.ack.redelivery.delay.ms";
    public const string ReceiverQueueSize = "pulsar.receiver.queue.size";
    public const string InitialPosition = "pulsar.initial.position";

    // Producer settings
    public const string ProducerName = "pulsar.producer.name";
    public const string SendTimeoutMs = "pulsar.send.timeout.ms";
    public const string BatchingEnabled = "pulsar.batching.enabled";
    public const string BatchingMaxMessages = "pulsar.batching.max.messages";
    public const string BatchingMaxDelayMs = "pulsar.batching.max.delay.ms";
    public const string CompressionType = "pulsar.compression.type";

    // Topic mapping
    public const string TopicMappingEnabled = "topic.mapping.enabled";
    public const string TopicMappingPrefix = "topic.mapping.prefix";

    // Defaults
    public const string DefaultServiceUrl = "pulsar://localhost:6650";
    public const string DefaultSubscription = "surgewave-pulsar-connector";
    public const string DefaultSubscriptionType = "Shared";
    public const string DefaultInitialPosition = "Earliest";
    public const int DefaultAckTimeoutMs = 30000;
    public const int DefaultReceiverQueueSize = 1000;
    public const int DefaultSendTimeoutMs = 30000;
    public const int DefaultBatchingMaxMessages = 1000;
    public const int DefaultBatchingMaxDelayMs = 10;
}
