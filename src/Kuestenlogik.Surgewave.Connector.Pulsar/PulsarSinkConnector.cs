using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Pulsar;

/// <summary>
/// Sink connector that produces to Apache Pulsar topics.
/// </summary>
[ConnectorMetadata(
    Name = "pulsar-sink",
    Description = "Produces messages to Apache Pulsar topics",
    Author = "Surgewave",
    Tags = "pulsar, sink, messaging, streaming")]
public sealed class PulsarSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(PulsarConnectorConfig.ServiceUrl, ConfigType.String, PulsarConnectorConfig.DefaultServiceUrl,
            Importance.High, "Pulsar service URL (e.g., pulsar://localhost:6650)")
        .Define(PulsarConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume", EditorHint.Topic)
        .Define(PulsarConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Pulsar topic to produce to (supports ${surgewave.topic} placeholder)", EditorHint.Topic)
        .Define(PulsarConnectorConfig.ProducerName, ConfigType.String, "", Importance.Low,
            "Pulsar producer name")
        .Define(PulsarConnectorConfig.SendTimeoutMs, ConfigType.Int,
            PulsarConnectorConfig.DefaultSendTimeoutMs.ToString(), Importance.Low,
            "Send timeout in milliseconds")
        .Define(PulsarConnectorConfig.BatchingEnabled, ConfigType.Boolean, "true", Importance.Medium,
            "Enable message batching")
        .Define(PulsarConnectorConfig.BatchingMaxMessages, ConfigType.Int,
            PulsarConnectorConfig.DefaultBatchingMaxMessages.ToString(), Importance.Medium,
            "Maximum messages per batch")
        .Define(PulsarConnectorConfig.BatchingMaxDelayMs, ConfigType.Int,
            PulsarConnectorConfig.DefaultBatchingMaxDelayMs.ToString(), Importance.Low,
            "Maximum batch delay in milliseconds")
        .Define(PulsarConnectorConfig.CompressionType, ConfigType.String, "None", Importance.Medium,
            "Compression type: None, LZ4, ZLib, ZStd, Snappy", EditorHint.Select, options: ["None", "LZ4", "Zlib", "Zstd", "Snappy"])
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
            "Prefix to add to Surgewave topic names");

    public override Type TaskClass => typeof(PulsarSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(PulsarConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{PulsarConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(PulsarConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{PulsarConnectorConfig.Topics}' is required");
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
