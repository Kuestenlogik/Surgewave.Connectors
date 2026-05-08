using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Sns;

/// <summary>
/// A sink connector that publishes messages from Surgewave topics
/// to AWS SNS topics.
/// </summary>
public sealed class SnsSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(SnsSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(SnsConnectorConfig.TopicArnConfig, ConfigType.String, Importance.High,
            "SNS topic ARN to publish messages to")
        .Define(SnsConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(SnsConnectorConfig.RegionConfig, ConfigType.String, SnsConnectorConfig.DefaultRegion, Importance.Medium,
            "AWS region")
        .Define(SnsConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS access key ID (optional, uses default credential chain if not specified)")
        .Define(SnsConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS secret access key")
        .Define(SnsConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low,
            "Custom endpoint URL (e.g., for LocalStack)")
        .Define(SnsConnectorConfig.SubjectConfig, ConfigType.String, "", Importance.Low,
            "Subject for SNS messages (optional)")
        .Define(SnsConnectorConfig.MessageGroupIdConfig, ConfigType.String, "", Importance.Medium,
            "Message group ID for FIFO topics")
        .Define(SnsConnectorConfig.HeaderPrefixConfig, ConfigType.String, SnsConnectorConfig.DefaultHeaderPrefix, Importance.Low,
            "Prefix for mapping Surgewave headers to SNS message attributes");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(SnsConnectorConfig.TopicArnConfig, out var topicArn) || string.IsNullOrWhiteSpace(topicArn))
            throw new ArgumentException($"Missing required config: {SnsConnectorConfig.TopicArnConfig}");

        if (!config.TryGetValue(SnsConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrWhiteSpace(topics))
            throw new ArgumentException($"Missing required config: {SnsConnectorConfig.TopicsConfig}");

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
