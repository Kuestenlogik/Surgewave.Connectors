using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Table;

/// <summary>
/// Sink connector that writes records to Azure Table Storage.
/// Supports upsert, insert, update, and delete operations with batch processing.
/// </summary>
public sealed class TableStorageSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(TableStorageSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(TableStorageConnectorConfig.ConnectionStringConfig, ConfigType.Password, "", Importance.High, "Table Storage connection string (preferred)")
        .Define(TableStorageConnectorConfig.AccountNameConfig, ConfigType.String, "", Importance.High, "Storage account name (alternative to connection string)")
        .Define(TableStorageConnectorConfig.AccountKeyConfig, ConfigType.Password, "", Importance.High, "Storage account key (used with account name)")
        .Define(TableStorageConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (for Azurite emulator)")
        .Define(TableStorageConnectorConfig.TableNameConfig, ConfigType.String, Importance.High, "Table name to write to")
        .Define(TableStorageConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(TableStorageConnectorConfig.WriteModeConfig, ConfigType.String, TableStorageConnectorConfig.WriteModeUpsert, Importance.Medium, "Write mode: upsert, insert, update, delete", EditorHint.Select, options: ["upsert", "insert", "update", "delete"])
        .Define(TableStorageConnectorConfig.PartitionKeyFieldConfig, ConfigType.String, "partitionKey", Importance.High, "Field to use as PartitionKey")
        .Define(TableStorageConnectorConfig.RowKeyFieldConfig, ConfigType.String, "rowKey", Importance.High, "Field to use as RowKey")
        .Define(TableStorageConnectorConfig.BatchSizeConfig, ConfigType.Int, TableStorageConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for table operations (max 100)")
        .Define(TableStorageConnectorConfig.AutoCreateTableConfig, ConfigType.Boolean, false, Importance.Low, "Auto-create table if not exists")
        .Define(TableStorageConnectorConfig.MaxRetryCountConfig, ConfigType.Int, TableStorageConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Max retry count for transient failures")
        .Define(TableStorageConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)TableStorageConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Retry delay in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        var hasConnectionString = config.TryGetValue(TableStorageConnectorConfig.ConnectionStringConfig, out var connStr) && !string.IsNullOrEmpty(connStr);
        var hasAccountName = config.TryGetValue(TableStorageConnectorConfig.AccountNameConfig, out var accountName) && !string.IsNullOrEmpty(accountName);

        if (!hasConnectionString && !hasAccountName)
            throw new ArgumentException($"Either '{TableStorageConnectorConfig.ConnectionStringConfig}' or '{TableStorageConnectorConfig.AccountNameConfig}' must be specified");

        if (!config.TryGetValue(TableStorageConnectorConfig.TableNameConfig, out var tableName) || string.IsNullOrEmpty(tableName))
            throw new ArgumentException($"Required configuration '{TableStorageConnectorConfig.TableNameConfig}' is missing");

        if (!config.TryGetValue(TableStorageConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{TableStorageConnectorConfig.TopicsConfig}' is missing");

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
