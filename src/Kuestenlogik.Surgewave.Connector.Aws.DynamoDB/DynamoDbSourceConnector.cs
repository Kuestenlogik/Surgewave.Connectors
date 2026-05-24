using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;

/// <summary>
/// Source connector that captures changes from DynamoDB Streams.
/// Reads INSERT, MODIFY, and REMOVE events and produces Debezium-compatible output.
/// </summary>
public sealed class DynamoDbSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(DynamoDbSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(DynamoDbConnectorConfig.StreamArnConfig, ConfigType.String, Importance.High, "DynamoDB Stream ARN to consume")
        .Define(DynamoDbConnectorConfig.TableNameConfig, ConfigType.String, "", Importance.Medium, "DynamoDB table name (extracted from ARN if not specified)")
        .Define(DynamoDbConnectorConfig.RegionConfig, ConfigType.String, DynamoDbConnectorConfig.DefaultRegion, Importance.Medium, "AWS region")
        .Define(DynamoDbConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS access key ID (optional, uses default credential chain)")
        .Define(DynamoDbConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS secret access key")
        .Define(DynamoDbConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (e.g., for LocalStack)")
        .Define(DynamoDbConnectorConfig.TopicPatternConfig, ConfigType.String, DynamoDbConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${table})")
        .Define(DynamoDbConnectorConfig.ShardIteratorTypeConfig, ConfigType.String, DynamoDbConnectorConfig.ShardIteratorLatest, Importance.Medium, "Shard iterator type (TRIM_HORIZON, LATEST)", EditorHint.Select, options: ["TRIM_HORIZON", "LATEST"])
        .Define(DynamoDbConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)DynamoDbConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in ms")
        .Define(DynamoDbConnectorConfig.BatchMaxRecordsConfig, ConfigType.Int, DynamoDbConnectorConfig.DefaultBatchMaxRecords, Importance.Low, "Max records per batch")
        .Define(DynamoDbConnectorConfig.StartFromBeginningConfig, ConfigType.Boolean, false, Importance.Medium, "Start from beginning of stream (TRIM_HORIZON)")
        .Define(DynamoDbConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include stream metadata in output");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(DynamoDbConnectorConfig.StreamArnConfig, out var streamArn) || string.IsNullOrEmpty(streamArn))
            throw new ArgumentException($"Required configuration '{DynamoDbConnectorConfig.StreamArnConfig}' is missing");

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
