using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis;

/// <summary>
/// Sink connector that writes records to AWS Kinesis Data Streams.
/// Supports batch writes with PutRecords for high throughput.
/// </summary>
public sealed class KinesisSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(KinesisSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(KinesisConnectorConfig.StreamNameConfig, ConfigType.String, Importance.High, "Kinesis stream name to write to")
        .Define(KinesisConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(KinesisConnectorConfig.RegionConfig, ConfigType.String, KinesisConnectorConfig.DefaultRegion, Importance.Medium, "AWS region")
        .Define(KinesisConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS access key ID (optional, uses default credential chain)")
        .Define(KinesisConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS secret access key")
        .Define(KinesisConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (e.g., for LocalStack)")
        .Define(KinesisConnectorConfig.PartitionKeyFieldConfig, ConfigType.String, "", Importance.Medium, "Field to use as partition key (uses record key if not set)")
        .Define(KinesisConnectorConfig.ExplicitHashKeyFieldConfig, ConfigType.String, "", Importance.Low, "Field to use as explicit hash key (optional)")
        .Define(KinesisConnectorConfig.BatchSizeConfig, ConfigType.Int, KinesisConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for writes (max 500)")
        .Define(KinesisConnectorConfig.RetryCountConfig, ConfigType.Int, KinesisConnectorConfig.DefaultRetryCount, Importance.Low, "Number of retries for failed records")
        .Define(KinesisConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)KinesisConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Delay between retries in ms");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(KinesisConnectorConfig.StreamNameConfig, out var streamName) || string.IsNullOrEmpty(streamName))
            throw new ArgumentException($"Required configuration '{KinesisConnectorConfig.StreamNameConfig}' is missing");

        if (!config.TryGetValue(KinesisConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{KinesisConnectorConfig.TopicsConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for simplicity
        return [new Dictionary<string, string>(_config)];
    }
}
