using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Gcp.PubSub;

/// <summary>
/// A sink connector that publishes messages from Surgewave topics
/// to Google Cloud Pub/Sub topics.
/// </summary>
public sealed class PubSubSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(PubSubSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(PubSubConnectorConfig.ProjectIdConfig, ConfigType.String, Importance.High,
            "Google Cloud project ID")
        .Define(PubSubConnectorConfig.PubSubTopicIdConfig, ConfigType.String, Importance.High,
            "Pub/Sub topic ID to publish messages to")
        .Define(PubSubConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(PubSubConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.Medium,
            "Service account JSON credentials (inline)")
        .Define(PubSubConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.Medium,
            "Path to service account JSON credentials file", EditorHint.FilePath)
        .Define(PubSubConnectorConfig.EmulatorHostConfig, ConfigType.String, "", Importance.Low,
            "Pub/Sub emulator host (e.g., localhost:8085) for local testing")
        .Define(PubSubConnectorConfig.OrderingKeyFieldConfig, ConfigType.String, "", Importance.Medium,
            "Header or field name to use as ordering key for FIFO delivery")
        .Define(PubSubConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)PubSubConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of messages to batch before publishing")
        .Define(PubSubConnectorConfig.BatchDelayMsConfig, ConfigType.Int, (long)PubSubConnectorConfig.DefaultBatchDelayMs, Importance.Medium,
            "Maximum delay in milliseconds before flushing a batch")
        .Define(PubSubConnectorConfig.HeaderPrefixConfig, ConfigType.String, PubSubConnectorConfig.DefaultHeaderPrefix, Importance.Low,
            "Prefix for mapping Surgewave headers to Pub/Sub attributes");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(PubSubConnectorConfig.ProjectIdConfig, out var projectId) || string.IsNullOrWhiteSpace(projectId))
            throw new ArgumentException($"Missing required config: {PubSubConnectorConfig.ProjectIdConfig}");

        if (!config.TryGetValue(PubSubConnectorConfig.PubSubTopicIdConfig, out var topicId) || string.IsNullOrWhiteSpace(topicId))
            throw new ArgumentException($"Missing required config: {PubSubConnectorConfig.PubSubTopicIdConfig}");

        if (!config.TryGetValue(PubSubConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrWhiteSpace(topics))
            throw new ArgumentException($"Missing required config: {PubSubConnectorConfig.TopicsConfig}");

        // Validate batch size
        var batchSize = GetConfigInt(config, PubSubConnectorConfig.BatchSizeConfig, PubSubConnectorConfig.DefaultBatchSize);
        if (batchSize is < 1 or > 1000)
            throw new ArgumentException($"Invalid batch size {batchSize}. Must be between 1 and 1000.");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task is sufficient as Pub/Sub handles batching and scaling
        return [new Dictionary<string, string>(_config)];
    }

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;
}
