using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Sqs;

/// <summary>
/// A source connector that receives messages from AWS SQS queues
/// and produces them to Surgewave topics.
/// </summary>
public sealed class SqsSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(SqsSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(SqsConnectorConfig.QueueUrlConfig, ConfigType.String, Importance.High,
            "SQS queue URL to receive messages from")
        .Define(SqsConnectorConfig.SurgewaveTopicConfig, ConfigType.String, Importance.High,
            "Destination Surgewave topic", EditorHint.Topic)
        .Define(SqsConnectorConfig.RegionConfig, ConfigType.String, SqsConnectorConfig.DefaultRegion, Importance.Medium,
            "AWS region")
        .Define(SqsConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS access key ID (optional, uses default credential chain if not specified)")
        .Define(SqsConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS secret access key")
        .Define(SqsConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low,
            "Custom endpoint URL (e.g., for LocalStack)")
        .Define(SqsConnectorConfig.WaitTimeSecondsConfig, ConfigType.Int, (long)SqsConnectorConfig.DefaultWaitTimeSeconds, Importance.Medium,
            "Long polling wait time in seconds (0-20)")
        .Define(SqsConnectorConfig.VisibilityTimeoutConfig, ConfigType.Int, (long)SqsConnectorConfig.DefaultVisibilityTimeout, Importance.Medium,
            "Message visibility timeout in seconds")
        .Define(SqsConnectorConfig.MaxMessagesConfig, ConfigType.Int, (long)SqsConnectorConfig.DefaultMaxMessages, Importance.Medium,
            "Maximum messages to receive per request (1-10)")
        .Define(SqsConnectorConfig.HeaderPrefixConfig, ConfigType.String, SqsConnectorConfig.DefaultHeaderPrefix, Importance.Low,
            "Prefix for SQS attributes in Surgewave headers")
        .Define(SqsConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, SqsConnectorConfig.DefaultIncludeMetadata, Importance.Low,
            "Include SQS metadata (messageId, receiptHandle) in headers");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(SqsConnectorConfig.QueueUrlConfig, out var queueUrl) || string.IsNullOrWhiteSpace(queueUrl))
            throw new ArgumentException($"Missing required config: {SqsConnectorConfig.QueueUrlConfig}");

        if (!config.TryGetValue(SqsConnectorConfig.SurgewaveTopicConfig, out var surgewaveTopic) || string.IsNullOrWhiteSpace(surgewaveTopic))
            throw new ArgumentException($"Missing required config: {SqsConnectorConfig.SurgewaveTopicConfig}");

        // Validate max messages
        var maxMessages = GetConfigInt(config, SqsConnectorConfig.MaxMessagesConfig, SqsConnectorConfig.DefaultMaxMessages);
        if (maxMessages is < 1 or > 10)
            throw new ArgumentException($"Invalid max messages {maxMessages}. Must be between 1 and 10.");

        // Validate wait time
        var waitTime = GetConfigInt(config, SqsConnectorConfig.WaitTimeSecondsConfig, SqsConnectorConfig.DefaultWaitTimeSeconds);
        if (waitTime is < 0 or > 20)
            throw new ArgumentException($"Invalid wait time {waitTime}. Must be between 0 and 20.");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task per queue - scaling is handled by SQS
        return [new Dictionary<string, string>(_config)];
    }

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;
}
