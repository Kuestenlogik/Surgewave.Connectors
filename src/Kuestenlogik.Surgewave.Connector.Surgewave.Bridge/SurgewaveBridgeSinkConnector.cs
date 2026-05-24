using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Surgewave.Bridge;

/// <summary>
/// Sink connector that writes records to a remote Surgewave cluster.
/// </summary>
[ConnectorMetadata(
    Name = "surgewave-bridge-sink",
    Description = "Writes records to a remote Surgewave cluster for cross-cluster replication",
    Author = "Surgewave",
    Tags = "surgewave, bridge, replication, mirror, sink")]
public sealed class SurgewaveBridgeSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(SurgewaveBridgeConnectorConfig.TargetBootstrapServers, ConfigType.String, Importance.High,
            "Bootstrap servers for the target Surgewave cluster")
        .Define(SurgewaveBridgeConnectorConfig.TargetClusterAlias, ConfigType.String,
            SurgewaveBridgeConnectorConfig.DefaultTargetClusterAlias, Importance.Medium,
            "Alias for the target cluster")
        .Define(SurgewaveBridgeConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Comma-separated list of topics to consume and forward", EditorHint.Topic)
        .Define(SurgewaveBridgeConnectorConfig.Topic, ConfigType.String, "", Importance.Medium,
            "Target topic override (supports ${topic} placeholder, empty = same as source)", EditorHint.Topic)
        .Define(SurgewaveBridgeConnectorConfig.TopicPrefixEnabled, ConfigType.Boolean, "false", Importance.Medium,
            "Whether to add target cluster prefix to topics")
        .Define(SurgewaveBridgeConnectorConfig.TopicPrefixSeparator, ConfigType.String,
            SurgewaveBridgeConnectorConfig.DefaultTopicPrefixSeparator, Importance.Low,
            "Separator between cluster alias and topic name")
        .Define(SurgewaveBridgeConnectorConfig.PreservePartitions, ConfigType.Boolean, "true", Importance.Medium,
            "Whether to preserve source partition assignments")
        .Define(SurgewaveBridgeConnectorConfig.BatchSize, ConfigType.Int,
            SurgewaveBridgeConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Number of records to batch before sending")
        .Define(SurgewaveBridgeConnectorConfig.ProducerLingerMs, ConfigType.Int,
            SurgewaveBridgeConnectorConfig.DefaultProducerLingerMs.ToString(), Importance.Low,
            "Producer linger time in milliseconds");

    public override Type TaskClass => typeof(SurgewaveBridgeSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(SurgewaveBridgeConnectorConfig.TargetBootstrapServers, out var servers) ||
            string.IsNullOrWhiteSpace(servers))
        {
            throw new ArgumentException($"'{SurgewaveBridgeConnectorConfig.TargetBootstrapServers}' is required");
        }

        if (!config.TryGetValue(SurgewaveBridgeConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{SurgewaveBridgeConnectorConfig.Topics}' is required");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
