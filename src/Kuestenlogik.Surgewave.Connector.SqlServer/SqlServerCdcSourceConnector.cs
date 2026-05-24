using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SqlServer;

/// <summary>
/// Source connector that captures changes from SQL Server using Change Data Capture (CDC).
/// Reads from CDC change tables populated by SQL Server's built-in CDC feature.
/// </summary>
public sealed class SqlServerCdcSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SqlServerCdcSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(SqlServerConnectorConfig.ConnectionString, ConfigType.String, "", Importance.High, "SQL Server connection string (alternative to individual settings)")
        .Define(SqlServerConnectorConfig.Server, ConfigType.String, SqlServerConnectorConfig.DefaultServer, Importance.High, "SQL Server hostname")
        .Define(SqlServerConnectorConfig.Database, ConfigType.String, Importance.High, "Database name")
        .Define(SqlServerConnectorConfig.Username, ConfigType.String, "", Importance.Medium, "Username (empty for Windows auth)")
        .Define(SqlServerConnectorConfig.Password, ConfigType.Password, "", Importance.Medium, "Password")
        .Define(SqlServerConnectorConfig.Tables, ConfigType.List, Importance.High, "Tables to capture (schema.table or table)")
        .Define(SqlServerConnectorConfig.TrustServerCertificate, ConfigType.Boolean, false, Importance.Medium, "Trust server certificate")
        .Define(SqlServerConnectorConfig.Encrypt, ConfigType.Boolean, true, Importance.Medium, "Encrypt connection")
        .Define(SqlServerConnectorConfig.TopicPrefix, ConfigType.String, "", Importance.Low, "Topic name prefix")
        .Define(SqlServerConnectorConfig.TopicPattern, ConfigType.String, SqlServerConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${schema}, ${table})")
        .Define(SqlServerConnectorConfig.SnapshotMode, ConfigType.String, SqlServerConnectorConfig.SnapshotModeInitial, Importance.Medium, "Snapshot mode (initial, never, always, schema_only)", EditorHint.Select, options: ["initial", "never", "always", "schema_only"])
        .Define(SqlServerConnectorConfig.IncludeSchema, ConfigType.Boolean, true, Importance.Low, "Include schema in topic name")
        .Define(SqlServerConnectorConfig.IncludeBeforeValues, ConfigType.Boolean, true, Importance.Low, "Include before values in updates")
        .Define(SqlServerConnectorConfig.PollIntervalMs, ConfigType.Int, (int)SqlServerConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in ms")
        .Define(SqlServerConnectorConfig.BatchMaxRecords, ConfigType.Int, SqlServerConnectorConfig.DefaultBatchMaxRecords, Importance.Low, "Max records per batch")
        .Define(SqlServerConnectorConfig.StartFromBeginning, ConfigType.Boolean, false, Importance.Medium, "Start from beginning of CDC history");

    public override void Start(IDictionary<string, string> config)
    {
        // Require either connection string or database name
        var hasConnectionString = config.TryGetValue(SqlServerConnectorConfig.ConnectionString, out var connStr) && !string.IsNullOrEmpty(connStr);
        var hasDatabase = config.TryGetValue(SqlServerConnectorConfig.Database, out var database) && !string.IsNullOrEmpty(database);

        if (!hasConnectionString && !hasDatabase)
            throw new ArgumentException($"Required configuration '{SqlServerConnectorConfig.Database}' or '{SqlServerConnectorConfig.ConnectionString}' is missing");

        if (!config.TryGetValue(SqlServerConnectorConfig.Tables, out var tables) || string.IsNullOrEmpty(tables))
            throw new ArgumentException($"Required configuration '{SqlServerConnectorConfig.Tables}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for CDC - polling is sequential
        return [new Dictionary<string, string>(_config)];
    }
}
