using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Pulsar;

/// <summary>
/// Source connector that consumes from Apache Pulsar topics.
/// </summary>
[ConnectorMetadata(
    Name = "pulsar-source",
    Description = "Consumes messages from Apache Pulsar topics",
    Author = "Surgewave",
    Tags = "pulsar, source, messaging, streaming")]
public sealed class PulsarSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(PulsarConnectorConfig.ServiceUrl, ConfigType.String, PulsarConnectorConfig.DefaultServiceUrl,
            Importance.High, "Pulsar service URL (e.g., pulsar://localhost:6650)")
        .Define(PulsarConnectorConfig.Topics, ConfigType.List, "", Importance.High,
            "Comma-separated list of Pulsar topics to consume", EditorHint.Topic)
        .Define(PulsarConnectorConfig.TopicsPattern, ConfigType.String, "", Importance.Medium,
            "Regex pattern for Pulsar topics")
        .Define(PulsarConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce to (supports ${pulsar.topic} placeholder)", EditorHint.Topic)
        .Define(PulsarConnectorConfig.Subscription, ConfigType.String, PulsarConnectorConfig.DefaultSubscription,
            Importance.High, "Pulsar subscription name")
        .Define(PulsarConnectorConfig.SubscriptionType, ConfigType.String, PulsarConnectorConfig.DefaultSubscriptionType,
            Importance.Medium, "Subscription type: Exclusive, Shared, Failover, KeyShared", EditorHint.Select, options: ["Exclusive", "Shared", "Failover", "Key_Shared"])
        .Define(PulsarConnectorConfig.InitialPosition, ConfigType.String, PulsarConnectorConfig.DefaultInitialPosition,
            Importance.Medium, "Initial position: Earliest, Latest", EditorHint.Select, options: ["Latest", "Earliest"])
        .Define(PulsarConnectorConfig.ConsumerName, ConfigType.String, "", Importance.Low,
            "Pulsar consumer name")
        .Define(PulsarConnectorConfig.AckTimeoutMs, ConfigType.Int,
            PulsarConnectorConfig.DefaultAckTimeoutMs.ToString(), Importance.Low,
            "Acknowledgment timeout in milliseconds")
        .Define(PulsarConnectorConfig.ReceiverQueueSize, ConfigType.Int,
            PulsarConnectorConfig.DefaultReceiverQueueSize.ToString(), Importance.Medium,
            "Receiver queue size")
        .Define(PulsarConnectorConfig.AuthPluginClassName, ConfigType.String, "", Importance.Medium,
            "Authentication plugin class name")
        .Define(PulsarConnectorConfig.AuthParams, ConfigType.String, "", Importance.Medium,
            "Authentication parameters (JSON)")
        .Define(PulsarConnectorConfig.TlsTrustCertsFilePath, ConfigType.String, "", Importance.Medium,
            "Path to TLS trust certificates", EditorHint.FilePath)
        .Define(PulsarConnectorConfig.TlsAllowInsecureConnection, ConfigType.Boolean, "false", Importance.Low,
            "Allow insecure TLS connections")
        .Define(PulsarConnectorConfig.TopicMappingEnabled, ConfigType.Boolean, "false", Importance.Medium,
            "Enable topic name mapping")
        .Define(PulsarConnectorConfig.TopicMappingPrefix, ConfigType.String, "", Importance.Low,
            "Prefix to add to Pulsar topic names");

    public override Type TaskClass => typeof(PulsarSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(PulsarConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{PulsarConnectorConfig.Topic}' is required");
        }

        var hasTopics = config.TryGetValue(PulsarConnectorConfig.Topics, out var topics) && !string.IsNullOrWhiteSpace(topics);
        var hasPattern = config.TryGetValue(PulsarConnectorConfig.TopicsPattern, out var pattern) && !string.IsNullOrWhiteSpace(pattern);

        if (!hasTopics && !hasPattern)
        {
            throw new ArgumentException($"Either '{PulsarConnectorConfig.Topics}' or '{PulsarConnectorConfig.TopicsPattern}' is required");
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
