using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Kafka.Bridge;

/// <summary>
/// Source connector that consumes from Apache Kafka and produces to Surgewave.
/// </summary>
[ConnectorMetadata(
    Name = "kafka-bridge-source",
    Description = "Bridges data from Apache Kafka to Surgewave by consuming Kafka topics",
    Author = "Surgewave",
    Tags = "kafka, bridge, source, migration, replication")]
public sealed class KafkaBridgeSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(KafkaBridgeConnectorConfig.KafkaBootstrapServers, ConfigType.String, Importance.High,
            "Kafka bootstrap servers")
        .Define(KafkaBridgeConnectorConfig.KafkaGroupId, ConfigType.String,
            KafkaBridgeConnectorConfig.DefaultGroupId, Importance.High,
            "Kafka consumer group ID")
        .Define(KafkaBridgeConnectorConfig.Topics, ConfigType.List, "", Importance.High,
            "Comma-separated list of Kafka topics to consume", EditorHint.Topic)
        .Define(KafkaBridgeConnectorConfig.TopicsPattern, ConfigType.String, "", Importance.Medium,
            "Regex pattern for Kafka topics")
        .Define(KafkaBridgeConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce to (supports ${kafka.topic} placeholder)", EditorHint.Topic)
        .Define(KafkaBridgeConnectorConfig.SecurityProtocol, ConfigType.String, "PLAINTEXT", Importance.Medium,
            "Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL", EditorHint.Select, options: ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"])
        .Define(KafkaBridgeConnectorConfig.SaslMechanism, ConfigType.String, "", Importance.Medium,
            "SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", EditorHint.Select, options: ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"])
        .Define(KafkaBridgeConnectorConfig.SaslUsername, ConfigType.String, "", Importance.Medium,
            "SASL username")
        .Define(KafkaBridgeConnectorConfig.SaslPassword, ConfigType.Password, "", Importance.Medium,
            "SASL password")
        .Define(KafkaBridgeConnectorConfig.AutoOffsetReset, ConfigType.String,
            KafkaBridgeConnectorConfig.DefaultAutoOffsetReset, Importance.Medium,
            "Auto offset reset: earliest, latest", EditorHint.Select, options: ["latest", "earliest", "none"])
        .Define(KafkaBridgeConnectorConfig.MaxPollRecords, ConfigType.Int,
            KafkaBridgeConnectorConfig.DefaultMaxPollRecords.ToString(), Importance.Medium,
            "Maximum records per poll")
        .Define(KafkaBridgeConnectorConfig.PollTimeoutMs, ConfigType.Int,
            KafkaBridgeConnectorConfig.DefaultPollTimeoutMs.ToString(), Importance.Low,
            "Poll timeout in milliseconds")
        .Define(KafkaBridgeConnectorConfig.TopicMappingEnabled, ConfigType.Boolean, "false", Importance.Medium,
            "Enable topic name mapping")
        .Define(KafkaBridgeConnectorConfig.TopicMappingPrefix, ConfigType.String, "", Importance.Low,
            "Prefix to add to Kafka topic names")
        .Define(KafkaBridgeConnectorConfig.TopicMappingSuffix, ConfigType.String, "", Importance.Low,
            "Suffix to add to Kafka topic names");

    public override Type TaskClass => typeof(KafkaBridgeSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(KafkaBridgeConnectorConfig.KafkaBootstrapServers, out var servers) ||
            string.IsNullOrWhiteSpace(servers))
        {
            throw new ArgumentException($"'{KafkaBridgeConnectorConfig.KafkaBootstrapServers}' is required");
        }

        if (!config.TryGetValue(KafkaBridgeConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{KafkaBridgeConnectorConfig.Topic}' is required");
        }

        var hasTopics = config.TryGetValue(KafkaBridgeConnectorConfig.Topics, out var topics) && !string.IsNullOrWhiteSpace(topics);
        var hasPattern = config.TryGetValue(KafkaBridgeConnectorConfig.TopicsPattern, out var pattern) && !string.IsNullOrWhiteSpace(pattern);

        if (!hasTopics && !hasPattern)
        {
            throw new ArgumentException($"Either '{KafkaBridgeConnectorConfig.Topics}' or '{KafkaBridgeConnectorConfig.TopicsPattern}' is required");
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
