using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Sqs;

/// <summary>
/// A sink connector that sends messages from Surgewave topics
/// to AWS SQS queues.
/// </summary>
public sealed class SqsSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(SqsSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(SqsConnectorConfig.QueueUrlConfig, ConfigType.String, Importance.High,
            "SQS queue URL to send messages to")
        .Define(SqsConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(SqsConnectorConfig.RegionConfig, ConfigType.String, SqsConnectorConfig.DefaultRegion, Importance.Medium,
            "AWS region")
        .Define(SqsConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS access key ID (optional, uses default credential chain if not specified)")
        .Define(SqsConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS secret access key")
        .Define(SqsConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low,
            "Custom endpoint URL (e.g., for LocalStack)")
        .Define(SqsConnectorConfig.MessageGroupIdFieldConfig, ConfigType.String, "", Importance.Medium,
            "Header or field name to use as MessageGroupId for FIFO queues")
        .Define(SqsConnectorConfig.DeduplicationIdFieldConfig, ConfigType.String, "", Importance.Medium,
            "Header or field name to use as MessageDeduplicationId for FIFO queues")
        .Define(SqsConnectorConfig.HeaderPrefixConfig, ConfigType.String, SqsConnectorConfig.DefaultHeaderPrefix, Importance.Low,
            "Prefix for mapping Surgewave headers to SQS message attributes");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(SqsConnectorConfig.QueueUrlConfig, out var queueUrl) || string.IsNullOrWhiteSpace(queueUrl))
            throw new ArgumentException($"Missing required config: {SqsConnectorConfig.QueueUrlConfig}");

        if (!config.TryGetValue(SqsConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrWhiteSpace(topics))
            throw new ArgumentException($"Missing required config: {SqsConnectorConfig.TopicsConfig}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task is sufficient
        return [new Dictionary<string, string>(_config)];
    }
}
