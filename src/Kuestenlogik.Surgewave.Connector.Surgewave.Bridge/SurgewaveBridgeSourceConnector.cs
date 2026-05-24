using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Surgewave.Bridge;

/// <summary>
/// Source connector that replicates topics from one Surgewave cluster to another.
/// </summary>
[ConnectorMetadata(
    Name = "surgewave-bridge-source",
    Description = "Replicates topics from a source Surgewave cluster to the local cluster",
    Author = "Surgewave",
    Tags = "surgewave, bridge, replication, mirror, source")]
public sealed class SurgewaveBridgeSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";
    private string _sourceBootstrapServers = null!;
    private string _sourceClusterAlias = null!;
    private List<string> _topics = [];
    private Regex? _topicsPattern;
    private HashSet<string> _topicsBlacklist = [];

    public override ConfigDef Config => new ConfigDef()
        .Define(SurgewaveBridgeConnectorConfig.SourceBootstrapServers, ConfigType.String, Importance.High,
            "Bootstrap servers for the source Surgewave cluster")
        .Define(SurgewaveBridgeConnectorConfig.SourceClusterAlias, ConfigType.String,
            SurgewaveBridgeConnectorConfig.DefaultSourceClusterAlias, Importance.Medium,
            "Alias for the source cluster (used in topic prefixing)")
        .Define(SurgewaveBridgeConnectorConfig.Topics, ConfigType.List, "", Importance.High,
            "Comma-separated list of topics to replicate", EditorHint.Topic)
        .Define(SurgewaveBridgeConnectorConfig.TopicsPattern, ConfigType.String, "", Importance.Medium,
            "Regex pattern to match topics for replication")
        .Define(SurgewaveBridgeConnectorConfig.TopicsBlacklist, ConfigType.List, "", Importance.Medium,
            "Comma-separated list of topics to exclude from replication")
        .Define(SurgewaveBridgeConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Target topic for replicated messages (supports ${source.topic} placeholder)", EditorHint.Topic)
        .Define(SurgewaveBridgeConnectorConfig.TopicPrefixEnabled, ConfigType.Boolean, "true", Importance.Medium,
            "Whether to prefix replicated topics with source cluster alias")
        .Define(SurgewaveBridgeConnectorConfig.TopicPrefixSeparator, ConfigType.String,
            SurgewaveBridgeConnectorConfig.DefaultTopicPrefixSeparator, Importance.Low,
            "Separator between cluster alias and topic name")
        .Define(SurgewaveBridgeConnectorConfig.PreservePartitions, ConfigType.Boolean, "true", Importance.Medium,
            "Whether to preserve source partition assignments")
        .Define(SurgewaveBridgeConnectorConfig.OffsetTrackingEnabled, ConfigType.Boolean, "true", Importance.Medium,
            "Whether to track offset mappings for failover")
        .Define(SurgewaveBridgeConnectorConfig.OffsetSyncIntervalMs, ConfigType.Int,
            SurgewaveBridgeConnectorConfig.DefaultOffsetSyncIntervalMs.ToString(), Importance.Low,
            "Interval for offset sync in milliseconds")
        .Define(SurgewaveBridgeConnectorConfig.StartFromLatest, ConfigType.Boolean, "false", Importance.Medium,
            "Whether to start from latest offset (true) or earliest (false)")
        .Define(SurgewaveBridgeConnectorConfig.BatchSize, ConfigType.Int,
            SurgewaveBridgeConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Maximum number of records to fetch per poll")
        .Define(SurgewaveBridgeConnectorConfig.PollTimeoutMs, ConfigType.Int,
            SurgewaveBridgeConnectorConfig.DefaultPollTimeoutMs.ToString(), Importance.Low,
            "Timeout for polling source cluster in milliseconds")
        .Define(SurgewaveBridgeConnectorConfig.HeartbeatEnabled, ConfigType.Boolean, "true", Importance.Low,
            "Whether to emit heartbeat records")
        .Define(SurgewaveBridgeConnectorConfig.HeartbeatIntervalMs, ConfigType.Int,
            SurgewaveBridgeConnectorConfig.DefaultHeartbeatIntervalMs.ToString(), Importance.Low,
            "Heartbeat interval in milliseconds");

    public override Type TaskClass => typeof(SurgewaveBridgeSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(SurgewaveBridgeConnectorConfig.SourceBootstrapServers, out _sourceBootstrapServers!) ||
            string.IsNullOrWhiteSpace(_sourceBootstrapServers))
        {
            throw new ArgumentException($"'{SurgewaveBridgeConnectorConfig.SourceBootstrapServers}' is required");
        }

        if (!config.TryGetValue(SurgewaveBridgeConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{SurgewaveBridgeConnectorConfig.Topic}' is required");
        }

        _sourceClusterAlias = config.TryGetValue(SurgewaveBridgeConnectorConfig.SourceClusterAlias, out var alias)
            ? alias : SurgewaveBridgeConnectorConfig.DefaultSourceClusterAlias;

        // Parse topics list
        if (config.TryGetValue(SurgewaveBridgeConnectorConfig.Topics, out var topicsStr) && !string.IsNullOrWhiteSpace(topicsStr))
        {
            _topics = topicsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        }

        // Parse topics pattern
        if (config.TryGetValue(SurgewaveBridgeConnectorConfig.TopicsPattern, out var pattern) && !string.IsNullOrWhiteSpace(pattern))
        {
            _topicsPattern = new Regex(pattern, RegexOptions.Compiled);
        }

        // Parse blacklist
        if (config.TryGetValue(SurgewaveBridgeConnectorConfig.TopicsBlacklist, out var blacklist) && !string.IsNullOrWhiteSpace(blacklist))
        {
            _topicsBlacklist = blacklist.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToHashSet();
        }

        if (_topics.Count == 0 && _topicsPattern == null)
        {
            throw new ArgumentException($"Either '{SurgewaveBridgeConnectorConfig.Topics}' or '{SurgewaveBridgeConnectorConfig.TopicsPattern}' must be specified");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // For now, single task handles all topics
        // Could partition topics across tasks for parallelism
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
