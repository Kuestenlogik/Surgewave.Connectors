using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis;

/// <summary>
/// Source connector that consumes records from AWS Kinesis Data Streams.
/// Reads from all shards and produces records with partition key and sequence number tracking.
/// </summary>
public sealed class KinesisSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(KinesisSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(KinesisConnectorConfig.StreamNameConfig, ConfigType.String, Importance.High, "Kinesis stream name to consume")
        .Define(KinesisConnectorConfig.RegionConfig, ConfigType.String, KinesisConnectorConfig.DefaultRegion, Importance.Medium, "AWS region")
        .Define(KinesisConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS access key ID (optional, uses default credential chain)")
        .Define(KinesisConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS secret access key")
        .Define(KinesisConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (e.g., for LocalStack)")
        .Define(KinesisConnectorConfig.TopicPatternConfig, ConfigType.String, KinesisConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${stream})")
        .Define(KinesisConnectorConfig.ShardIteratorTypeConfig, ConfigType.String, KinesisConnectorConfig.ShardIteratorLatest, Importance.Medium, "Shard iterator type (TRIM_HORIZON, LATEST, AT_TIMESTAMP)", EditorHint.Select, options: ["TRIM_HORIZON", "LATEST", "AT_TIMESTAMP"])
        .Define(KinesisConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)KinesisConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in ms")
        .Define(KinesisConnectorConfig.BatchMaxRecordsConfig, ConfigType.Int, KinesisConnectorConfig.DefaultBatchMaxRecords, Importance.Low, "Max records per batch (max 10000)")
        .Define(KinesisConnectorConfig.StartFromBeginningConfig, ConfigType.Boolean, false, Importance.Medium, "Start from beginning of stream (TRIM_HORIZON)")
        .Define(KinesisConnectorConfig.StartTimestampConfig, ConfigType.String, "", Importance.Low, "Start timestamp (ISO 8601) for AT_TIMESTAMP iterator")
        .Define(KinesisConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include stream metadata in output");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(KinesisConnectorConfig.StreamNameConfig, out var streamName) || string.IsNullOrEmpty(streamName))
            throw new ArgumentException($"Required configuration '{KinesisConnectorConfig.StreamNameConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - shard discovery and reading is handled within the task
        return [new Dictionary<string, string>(_config)];
    }
}
