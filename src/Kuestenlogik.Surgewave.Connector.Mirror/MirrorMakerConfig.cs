namespace Kuestenlogik.Surgewave.Connector.Mirror;

/// <summary>
/// Configuration for MirrorMaker cross-cluster replication.
/// </summary>
public sealed class MirrorMakerConfig
{
    // Cluster identifiers
    public required string SourceClusterAlias { get; init; }
    public required string TargetClusterAlias { get; init; }

    // Connection settings
    public required string SourceBootstrapServers { get; init; }
    public required string TargetBootstrapServers { get; init; }

    // Security - Source
    public string? SourceSecurityProtocol { get; init; }
    public string? SourceSaslMechanism { get; init; }
    public string? SourceSaslUsername { get; init; }
    public string? SourceSaslPassword { get; init; }

    // Security - Target
    public string? TargetSecurityProtocol { get; init; }
    public string? TargetSaslMechanism { get; init; }
    public string? TargetSaslUsername { get; init; }
    public string? TargetSaslPassword { get; init; }

    // Replication behavior
    public bool SyncTopicConfigs { get; init; } = true;
    public bool SyncTopicAcls { get; init; } = false;
    public bool SyncSchemas { get; init; } = false;
    public bool EmitHeartbeats { get; init; } = true;
    public bool EmitCheckpoints { get; init; } = true;

    // Topic filtering
    public string TopicsPattern { get; init; } = ".*";
    public IReadOnlyList<string> TopicsWhitelist { get; init; } = [];
    public IReadOnlyList<string> TopicsBlacklist { get; init; } = [];

    // Consumer group filtering for offset sync
    public string GroupsPattern { get; init; } = ".*";
    public IReadOnlyList<string> GroupsWhitelist { get; init; } = [];
    public IReadOnlyList<string> GroupsBlacklist { get; init; } = [];

    // Performance tuning
    public int TasksMax { get; init; } = 1;
    public int ConsumerPollTimeoutMs { get; init; } = 1000;
    public int ProducerBatchSize { get; init; } = 16384;
    public int ProducerLingerMs { get; init; } = 0;
    public int FetchMaxBytes { get; init; } = 52428800;
    public int FetchMinBytes { get; init; } = 1;
    public int MaxPollRecords { get; init; } = 500;

    // Offset sync
    public int OffsetSyncIntervalMs { get; init; } = 60000;
    public bool SyncGroupOffsets { get; init; } = true;

    // Heartbeat
    public int HeartbeatIntervalMs { get; init; } = 1000;
    public string HeartbeatsTopic { get; init; } = "heartbeats";

    // Checkpoint
    public int CheckpointIntervalMs { get; init; } = 60000;
    public string CheckpointsTopic { get; init; } = "checkpoints.internal";

    // Topic naming policy
    public string ReplicationPolicyClass { get; init; } =
        "Kuestenlogik.Surgewave.Connect.Mirror.Policies.DefaultReplicationPolicy";
    public string ReplicationPolicySeparator { get; init; } = ".";

    // Refresh intervals
    public int TopicRefreshIntervalMs { get; init; } = 30000;
    public int GroupRefreshIntervalMs { get; init; } = 60000;

    /// <summary>
    /// Parse configuration from a dictionary.
    /// </summary>
    public static MirrorMakerConfig FromDictionary(IDictionary<string, string> config)
    {
        return new MirrorMakerConfig
        {
            SourceClusterAlias = Get(config, "source.cluster.alias", "source"),
            TargetClusterAlias = Get(config, "target.cluster.alias", "target"),
            SourceBootstrapServers = Get(config, "source.bootstrap.servers", ""),
            TargetBootstrapServers = Get(config, "target.bootstrap.servers", ""),
            SourceSecurityProtocol = GetOrNull(config, "source.security.protocol"),
            SourceSaslMechanism = GetOrNull(config, "source.sasl.mechanism"),
            SourceSaslUsername = GetOrNull(config, "source.sasl.username"),
            SourceSaslPassword = GetOrNull(config, "source.sasl.password"),
            TargetSecurityProtocol = GetOrNull(config, "target.security.protocol"),
            TargetSaslMechanism = GetOrNull(config, "target.sasl.mechanism"),
            TargetSaslUsername = GetOrNull(config, "target.sasl.username"),
            TargetSaslPassword = GetOrNull(config, "target.sasl.password"),
            SyncTopicConfigs = bool.Parse(Get(config, "sync.topic.configs.enabled", "true")),
            SyncTopicAcls = bool.Parse(Get(config, "sync.topic.acls.enabled", "false")),
            SyncSchemas = bool.Parse(Get(config, "sync.schemas.enabled", "false")),
            EmitHeartbeats = bool.Parse(Get(config, "emit.heartbeats.enabled", "true")),
            EmitCheckpoints = bool.Parse(Get(config, "emit.checkpoints.enabled", "true")),
            TopicsPattern = Get(config, "topics", ".*"),
            TopicsWhitelist = ParseList(Get(config, "topics.whitelist", "")),
            TopicsBlacklist = ParseList(Get(config, "topics.blacklist", "")),
            GroupsPattern = Get(config, "groups", ".*"),
            GroupsWhitelist = ParseList(Get(config, "groups.whitelist", "")),
            GroupsBlacklist = ParseList(Get(config, "groups.blacklist", "")),
            TasksMax = int.Parse(Get(config, "tasks.max", "1")),
            ConsumerPollTimeoutMs = int.Parse(Get(config, "consumer.poll.timeout.ms", "1000")),
            ProducerBatchSize = int.Parse(Get(config, "producer.batch.size", "16384")),
            ProducerLingerMs = int.Parse(Get(config, "producer.linger.ms", "0")),
            FetchMaxBytes = int.Parse(Get(config, "fetch.max.bytes", "52428800")),
            FetchMinBytes = int.Parse(Get(config, "fetch.min.bytes", "1")),
            MaxPollRecords = int.Parse(Get(config, "max.poll.records", "500")),
            OffsetSyncIntervalMs = int.Parse(Get(config, "offset.sync.interval.ms", "60000")),
            SyncGroupOffsets = bool.Parse(Get(config, "sync.group.offsets.enabled", "true")),
            HeartbeatIntervalMs = int.Parse(Get(config, "heartbeats.interval.ms", "1000")),
            HeartbeatsTopic = Get(config, "heartbeats.topic", "heartbeats"),
            CheckpointIntervalMs = int.Parse(Get(config, "checkpoints.interval.ms", "60000")),
            CheckpointsTopic = Get(config, "checkpoints.topic", "checkpoints.internal"),
            ReplicationPolicyClass = Get(config, "replication.policy.class",
                "Kuestenlogik.Surgewave.Connect.Mirror.Policies.DefaultReplicationPolicy"),
            ReplicationPolicySeparator = Get(config, "replication.policy.separator", "."),
            TopicRefreshIntervalMs = int.Parse(Get(config, "topic.refresh.interval.ms", "30000")),
            GroupRefreshIntervalMs = int.Parse(Get(config, "group.refresh.interval.ms", "60000"))
        };
    }

    private static string Get(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private static string? GetOrNull(IDictionary<string, string> config, string key)
        => config.TryGetValue(key, out var value) ? value : null;

    private static string[] ParseList(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return [];

        return value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }
}
