using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Queue;

/// <summary>
/// Source connector that receives messages from Azure Queue Storage.
/// Supports polling-based message retrieval with visibility timeout.
/// </summary>
public sealed class QueueStorageSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(QueueStorageSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(QueueStorageConnectorConfig.ConnectionStringConfig, ConfigType.Password, "", Importance.High, "Queue Storage connection string (preferred)")
        .Define(QueueStorageConnectorConfig.AccountNameConfig, ConfigType.String, "", Importance.High, "Storage account name (alternative to connection string)")
        .Define(QueueStorageConnectorConfig.AccountKeyConfig, ConfigType.Password, "", Importance.High, "Storage account key (used with account name)")
        .Define(QueueStorageConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (for Azurite emulator)")
        .Define(QueueStorageConnectorConfig.QueueNameConfig, ConfigType.String, Importance.High, "Queue name to receive from")
        .Define(QueueStorageConnectorConfig.TopicPatternConfig, ConfigType.String, QueueStorageConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${queue})")
        .Define(QueueStorageConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)QueueStorageConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in milliseconds")
        .Define(QueueStorageConnectorConfig.MaxMessagesPerPollConfig, ConfigType.Int, QueueStorageConnectorConfig.DefaultMaxMessagesPerPoll, Importance.Low, "Maximum messages per poll (max 32)")
        .Define(QueueStorageConnectorConfig.VisibilityTimeoutSecondsConfig, ConfigType.Int, QueueStorageConnectorConfig.DefaultVisibilityTimeoutSeconds, Importance.Low, "Visibility timeout in seconds")
        .Define(QueueStorageConnectorConfig.DeleteAfterReadConfig, ConfigType.Boolean, false, Importance.Medium, "Delete messages after reading (before commit)")
        .Define(QueueStorageConnectorConfig.Base64DecodeConfig, ConfigType.Boolean, true, Importance.Low, "Decode Base64 message content")
        .Define(QueueStorageConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include Queue Storage metadata in output");

    public override void Start(IDictionary<string, string> config)
    {
        var hasConnectionString = config.TryGetValue(QueueStorageConnectorConfig.ConnectionStringConfig, out var connStr) && !string.IsNullOrEmpty(connStr);
        var hasAccountName = config.TryGetValue(QueueStorageConnectorConfig.AccountNameConfig, out var accountName) && !string.IsNullOrEmpty(accountName);

        if (!hasConnectionString && !hasAccountName)
            throw new ArgumentException($"Either '{QueueStorageConnectorConfig.ConnectionStringConfig}' or '{QueueStorageConnectorConfig.AccountNameConfig}' must be specified");

        if (!config.TryGetValue(QueueStorageConnectorConfig.QueueNameConfig, out var queueName) || string.IsNullOrEmpty(queueName))
            throw new ArgumentException($"Required configuration '{QueueStorageConnectorConfig.QueueNameConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - Queue Storage doesn't support multiple consumers well
        return [new Dictionary<string, string>(_config)];
    }
}
