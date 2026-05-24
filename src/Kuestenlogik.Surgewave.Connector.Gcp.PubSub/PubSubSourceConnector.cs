using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.PubSub;

/// <summary>
/// A source connector that pulls messages from Google Cloud Pub/Sub subscriptions
/// and produces them to Surgewave topics.
/// </summary>
public sealed class PubSubSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(PubSubSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(PubSubConnectorConfig.ProjectIdConfig, ConfigType.String, Importance.High,
            "Google Cloud project ID")
        .Define(PubSubConnectorConfig.SubscriptionIdConfig, ConfigType.String, Importance.High,
            "Pub/Sub subscription ID to pull messages from")
        .Define(PubSubConnectorConfig.SurgewaveTopicConfig, ConfigType.String, Importance.High,
            "Destination Surgewave topic", EditorHint.Topic)
        .Define(PubSubConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.Medium,
            "Service account JSON credentials (inline)")
        .Define(PubSubConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.Medium,
            "Path to service account JSON credentials file", EditorHint.FilePath)
        .Define(PubSubConnectorConfig.EmulatorHostConfig, ConfigType.String, "", Importance.Low,
            "Pub/Sub emulator host (e.g., localhost:8085) for local testing")
        .Define(PubSubConnectorConfig.MaxMessagesConfig, ConfigType.Int, (long)PubSubConnectorConfig.DefaultMaxMessages, Importance.Medium,
            "Maximum messages to pull per request")
        .Define(PubSubConnectorConfig.AckDeadlineSecondsConfig, ConfigType.Int, (long)PubSubConnectorConfig.DefaultAckDeadlineSeconds, Importance.Medium,
            "Acknowledgment deadline in seconds")
        .Define(PubSubConnectorConfig.AutoAckConfig, ConfigType.Boolean, PubSubConnectorConfig.DefaultAutoAck, Importance.Medium,
            "Automatically acknowledge messages after pull (vs. commit-based ack)")
        .Define(PubSubConnectorConfig.HeaderPrefixConfig, ConfigType.String, PubSubConnectorConfig.DefaultHeaderPrefix, Importance.Low,
            "Prefix for Pub/Sub attributes in Surgewave headers")
        .Define(PubSubConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, PubSubConnectorConfig.DefaultIncludeMetadata, Importance.Low,
            "Include Pub/Sub metadata (messageId, publishTime) in headers");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(PubSubConnectorConfig.ProjectIdConfig, out var projectId) || string.IsNullOrWhiteSpace(projectId))
            throw new ArgumentException($"Missing required config: {PubSubConnectorConfig.ProjectIdConfig}");

        if (!config.TryGetValue(PubSubConnectorConfig.SubscriptionIdConfig, out var subscriptionId) || string.IsNullOrWhiteSpace(subscriptionId))
            throw new ArgumentException($"Missing required config: {PubSubConnectorConfig.SubscriptionIdConfig}");

        if (!config.TryGetValue(PubSubConnectorConfig.SurgewaveTopicConfig, out var surgewaveTopic) || string.IsNullOrWhiteSpace(surgewaveTopic))
            throw new ArgumentException($"Missing required config: {PubSubConnectorConfig.SurgewaveTopicConfig}");

        // Validate max messages
        var maxMessages = GetConfigInt(config, PubSubConnectorConfig.MaxMessagesConfig, PubSubConnectorConfig.DefaultMaxMessages);
        if (maxMessages is < 1 or > 1000)
            throw new ArgumentException($"Invalid max messages {maxMessages}. Must be between 1 and 1000.");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Pub/Sub subscriptions can be scaled by Pub/Sub itself,
        // so a single task is sufficient. Multiple tasks would share
        // the same subscription and divide the messages.
        return [new Dictionary<string, string>(_config)];
    }

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;
}
