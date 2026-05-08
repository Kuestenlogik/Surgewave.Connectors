using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Kafka.Bridge;

/// <summary>
/// Sink connector that consumes from Surgewave and produces to Apache Kafka.
/// </summary>
[ConnectorMetadata(
    Name = "kafka-bridge-sink",
    Description = "Bridges data from Surgewave to Apache Kafka by producing to Kafka topics",
    Author = "Surgewave",
    Tags = "kafka, bridge, sink, migration, replication")]
public sealed class KafkaBridgeSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(KafkaBridgeConnectorConfig.KafkaBootstrapServers, ConfigType.String, Importance.High,
            "Kafka bootstrap servers")
        .Define(KafkaBridgeConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume", EditorHint.Topic)
        .Define(KafkaBridgeConnectorConfig.Topic, ConfigType.String, "", Importance.Medium,
            "Kafka topic to produce to (supports ${surgewave.topic} placeholder, empty = same as source)", EditorHint.Topic)
        .Define(KafkaBridgeConnectorConfig.SecurityProtocol, ConfigType.String, "PLAINTEXT", Importance.Medium,
            "Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL", EditorHint.Select, options: ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
        .Define(KafkaBridgeConnectorConfig.SaslMechanism, ConfigType.String, "", Importance.Medium,
            "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", EditorHint.Select, options: ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"])
        .Define(KafkaBridgeConnectorConfig.SaslUsername, ConfigType.String, "", Importance.Medium,
            "SASL username")
        .Define(KafkaBridgeConnectorConfig.SaslPassword, ConfigType.Password, "", Importance.Medium,
            "SASL password")
        .Define(KafkaBridgeConnectorConfig.Acks, ConfigType.String, KafkaBridgeConnectorConfig.DefaultAcks,
            Importance.Medium, "Producer acknowledgments: 0, 1, all", EditorHint.Select, options: ["0", "1", "all"])
        .Define(KafkaBridgeConnectorConfig.LingerMs, ConfigType.Int,
            KafkaBridgeConnectorConfig.DefaultLingerMs.ToString(), Importance.Low,
            "Producer linger time in milliseconds")
        .Define(KafkaBridgeConnectorConfig.BatchSize, ConfigType.Int,
            KafkaBridgeConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Producer batch size in bytes")
        .Define(KafkaBridgeConnectorConfig.CompressionType, ConfigType.String, "none", Importance.Medium,
            "Compression type: none, gzip, snappy, lz4, zstd", EditorHint.Select, options: ["none", "gzip", "snappy", "lz4", "zstd"])
        .Define(KafkaBridgeConnectorConfig.TopicMappingEnabled, ConfigType.Boolean, "false", Importance.Medium,
            "Enable topic name mapping")
        .Define(KafkaBridgeConnectorConfig.TopicMappingPrefix, ConfigType.String, "", Importance.Low,
            "Prefix to add to Surgewave topic names")
        .Define(KafkaBridgeConnectorConfig.TopicMappingSuffix, ConfigType.String, "", Importance.Low,
            "Suffix to add to Surgewave topic names");

    public override Type TaskClass => typeof(KafkaBridgeSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(KafkaBridgeConnectorConfig.KafkaBootstrapServers, out var servers) ||
            string.IsNullOrWhiteSpace(servers))
        {
            throw new ArgumentException($"'{KafkaBridgeConnectorConfig.KafkaBootstrapServers}' is required");
        }

        if (!config.TryGetValue(KafkaBridgeConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{KafkaBridgeConnectorConfig.Topics}' is required");
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
