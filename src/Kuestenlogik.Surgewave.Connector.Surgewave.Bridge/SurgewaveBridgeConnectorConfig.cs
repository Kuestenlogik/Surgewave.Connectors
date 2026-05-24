namespace Kuestenlogik.Surgewave.Connector.Surgewave.Bridge;

/// <summary>
/// Configuration constants for Surgewave Bridge connector.
/// </summary>
public static class SurgewaveBridgeConnectorConfig
{
    // Source cluster
    public const string SourceBootstrapServers = "source.bootstrap.servers";
    public const string SourceClusterAlias = "source.cluster.alias";

    // Target cluster
    public const string TargetBootstrapServers = "target.bootstrap.servers";
    public const string TargetClusterAlias = "target.cluster.alias";

    // Topic configuration
    public const string Topic = "topic";
    public const string Topics = "topics";
    public const string TopicsPattern = "topics.pattern";
    public const string TopicsBlacklist = "topics.blacklist";

    // Replication settings
    public const string ReplicationMode = "replication.mode";  // source, sink, bidirectional
    public const string TopicPrefixEnabled = "topic.prefix.enabled";
    public const string TopicPrefixSeparator = "topic.prefix.separator";
    public const string PreservePartitions = "preserve.partitions";

    // Offset tracking
    public const string OffsetTrackingEnabled = "offset.tracking.enabled";
    public const string OffsetSyncIntervalMs = "offset.sync.interval.ms";
    public const string StartFromLatest = "start.from.latest";

    // Performance
    public const string BatchSize = "batch.size";
    public const string PollTimeoutMs = "poll.timeout.ms";
    public const string ProducerLingerMs = "producer.linger.ms";

    // Heartbeat
    public const string HeartbeatEnabled = "heartbeat.enabled";
    public const string HeartbeatIntervalMs = "heartbeat.interval.ms";

    // Defaults
    public const string DefaultSourceClusterAlias = "source";
    public const string DefaultTargetClusterAlias = "target";
    public const string DefaultReplicationMode = "source";
    public const string DefaultTopicPrefixSeparator = ".";
    public const int DefaultBatchSize = 500;
    public const int DefaultPollTimeoutMs = 1000;
    public const int DefaultProducerLingerMs = 5;
    public const int DefaultOffsetSyncIntervalMs = 60000;
    public const int DefaultHeartbeatIntervalMs = 1000;
}
