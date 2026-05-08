using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Queue;

/// <summary>
/// Sink connector that sends messages to Azure Queue Storage.
/// Supports batch sending with configurable TTL.
/// </summary>
public sealed class QueueStorageSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(QueueStorageSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(QueueStorageConnectorConfig.ConnectionStringConfig, ConfigType.Password, "", Importance.High, "Queue Storage connection string (preferred)")
        .Define(QueueStorageConnectorConfig.AccountNameConfig, ConfigType.String, "", Importance.High, "Storage account name (alternative to connection string)")
        .Define(QueueStorageConnectorConfig.AccountKeyConfig, ConfigType.Password, "", Importance.High, "Storage account key (used with account name)")
        .Define(QueueStorageConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (for Azurite emulator)")
        .Define(QueueStorageConnectorConfig.QueueNameConfig, ConfigType.String, Importance.High, "Queue name to send to")
        .Define(QueueStorageConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(QueueStorageConnectorConfig.TimeToLiveSecondsConfig, ConfigType.Int, QueueStorageConnectorConfig.DefaultTimeToLiveSeconds, Importance.Low, "Message TTL in seconds (-1 for never)")
        .Define(QueueStorageConnectorConfig.BatchSizeConfig, ConfigType.Int, QueueStorageConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for sending")
        .Define(QueueStorageConnectorConfig.Base64EncodeConfig, ConfigType.Boolean, true, Importance.Low, "Base64 encode message content")
        .Define(QueueStorageConnectorConfig.AutoCreateQueueConfig, ConfigType.Boolean, false, Importance.Low, "Auto-create queue if not exists")
        .Define(QueueStorageConnectorConfig.MaxRetryCountConfig, ConfigType.Int, QueueStorageConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Max retry count for failures")
        .Define(QueueStorageConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)QueueStorageConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Retry delay in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        var hasConnectionString = config.TryGetValue(QueueStorageConnectorConfig.ConnectionStringConfig, out var connStr) && !string.IsNullOrEmpty(connStr);
        var hasAccountName = config.TryGetValue(QueueStorageConnectorConfig.AccountNameConfig, out var accountName) && !string.IsNullOrEmpty(accountName);

        if (!hasConnectionString && !hasAccountName)
            throw new ArgumentException($"Either '{QueueStorageConnectorConfig.ConnectionStringConfig}' or '{QueueStorageConnectorConfig.AccountNameConfig}' must be specified");

        if (!config.TryGetValue(QueueStorageConnectorConfig.QueueNameConfig, out var queueName) || string.IsNullOrEmpty(queueName))
            throw new ArgumentException($"Required configuration '{QueueStorageConnectorConfig.QueueNameConfig}' is missing");

        if (!config.TryGetValue(QueueStorageConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{QueueStorageConnectorConfig.TopicsConfig}' is missing");

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
