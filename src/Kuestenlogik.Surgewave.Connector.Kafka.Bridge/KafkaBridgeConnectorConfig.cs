namespace Kuestenlogik.Surgewave.Connector.Kafka.Bridge;

/// <summary>
/// Configuration constants for Kafka Bridge connector.
/// </summary>
public static class KafkaBridgeConnectorConfig
{
    // Kafka cluster
    public const string KafkaBootstrapServers = "kafka.bootstrap.servers";
    public const string KafkaGroupId = "kafka.group.id";

    // Security
    public const string SecurityProtocol = "security.protocol";
    public const string SaslMechanism = "sasl.mechanism";
    public const string SaslUsername = "sasl.username";
    public const string SaslPassword = "sasl.password";

    // Topic configuration
    public const string Topic = "topic";
    public const string Topics = "topics";
    public const string TopicsPattern = "topics.pattern";

    // Direction
    public const string Direction = "direction";  // kafka-to-surgewave, surgewave-to-kafka

    // Topic mapping
    public const string TopicMappingEnabled = "topic.mapping.enabled";
    public const string TopicMappingPrefix = "topic.mapping.prefix";
    public const string TopicMappingSuffix = "topic.mapping.suffix";

    // Consumer settings (for Kafka source)
    public const string AutoOffsetReset = "auto.offset.reset";
    public const string EnableAutoCommit = "enable.auto.commit";
    public const string MaxPollRecords = "max.poll.records";
    public const string PollTimeoutMs = "poll.timeout.ms";

    // Producer settings (for Kafka sink)
    public const string Acks = "acks";
    public const string LingerMs = "linger.ms";
    public const string BatchSize = "batch.size";
    public const string CompressionType = "compression.type";

    // Defaults
    public const string DefaultGroupId = "surgewave-kafka-bridge";
    public const string DefaultDirection = "kafka-to-surgewave";
    public const string DefaultAutoOffsetReset = "earliest";
    public const string DefaultAcks = "all";
    public const int DefaultMaxPollRecords = 500;
    public const int DefaultPollTimeoutMs = 1000;
    public const int DefaultLingerMs = 5;
    public const int DefaultBatchSize = 16384;
}
