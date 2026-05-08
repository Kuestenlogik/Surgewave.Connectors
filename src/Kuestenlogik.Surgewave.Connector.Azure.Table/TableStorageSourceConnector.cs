using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Table;

/// <summary>
/// Source connector that reads entities from Azure Table Storage.
/// Supports polling-based reads with optional incremental tracking.
/// </summary>
public sealed class TableStorageSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(TableStorageSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(TableStorageConnectorConfig.ConnectionStringConfig, ConfigType.Password, "", Importance.High, "Table Storage connection string (preferred)")
        .Define(TableStorageConnectorConfig.AccountNameConfig, ConfigType.String, "", Importance.High, "Storage account name (alternative to connection string)")
        .Define(TableStorageConnectorConfig.AccountKeyConfig, ConfigType.Password, "", Importance.High, "Storage account key (used with account name)")
        .Define(TableStorageConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (for Azurite emulator)")
        .Define(TableStorageConnectorConfig.TableNameConfig, ConfigType.String, Importance.High, "Table name to read from")
        .Define(TableStorageConnectorConfig.TopicPatternConfig, ConfigType.String, TableStorageConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${table})")
        .Define(TableStorageConnectorConfig.QueryFilterConfig, ConfigType.String, "", Importance.Low, "OData filter expression for querying entities", EditorHint.Code, "odata")
        .Define(TableStorageConnectorConfig.SelectColumnsConfig, ConfigType.String, "", Importance.Low, "Comma-separated list of columns to select")
        .Define(TableStorageConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)TableStorageConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in milliseconds")
        .Define(TableStorageConnectorConfig.MaxEntitiesPerPollConfig, ConfigType.Int, TableStorageConnectorConfig.DefaultMaxEntitiesPerPoll, Importance.Low, "Maximum entities per poll")
        .Define(TableStorageConnectorConfig.IncrementalModeConfig, ConfigType.String, TableStorageConnectorConfig.IncrementalModeNone, Importance.Medium, "Incremental mode: none, timestamp, rowkey", EditorHint.Select, options: ["none", "timestamp", "rowkey"])
        .Define(TableStorageConnectorConfig.IncrementalColumnConfig, ConfigType.String, TableStorageConnectorConfig.DefaultIncrementalColumn, Importance.Low, "Column for incremental tracking")
        .Define(TableStorageConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include Table Storage metadata in output");

    public override void Start(IDictionary<string, string> config)
    {
        var hasConnectionString = config.TryGetValue(TableStorageConnectorConfig.ConnectionStringConfig, out var connStr) && !string.IsNullOrEmpty(connStr);
        var hasAccountName = config.TryGetValue(TableStorageConnectorConfig.AccountNameConfig, out var accountName) && !string.IsNullOrEmpty(accountName);

        if (!hasConnectionString && !hasAccountName)
            throw new ArgumentException($"Either '{TableStorageConnectorConfig.ConnectionStringConfig}' or '{TableStorageConnectorConfig.AccountNameConfig}' must be specified");

        if (!config.TryGetValue(TableStorageConnectorConfig.TableNameConfig, out var tableName) || string.IsNullOrEmpty(tableName))
            throw new ArgumentException($"Required configuration '{TableStorageConnectorConfig.TableNameConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - Table Storage doesn't have built-in partitioning for CDC
        return [new Dictionary<string, string>(_config)];
    }
}
