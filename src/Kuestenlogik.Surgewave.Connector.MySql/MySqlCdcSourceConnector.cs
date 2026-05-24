using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.MySql;

/// <summary>
/// Source connector that captures changes from MySQL/MariaDB using binary log (binlog) replication.
/// Supports GTID-based and file/position-based binlog tracking.
/// </summary>
public sealed class MySqlCdcSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(MySqlCdcSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(MySqlConnectorConfig.Host, ConfigType.String, MySqlConnectorConfig.DefaultHost, Importance.High, "MySQL host")
        .Define(MySqlConnectorConfig.Port, ConfigType.Int, MySqlConnectorConfig.DefaultPort, Importance.Medium, "MySQL port")
        .Define(MySqlConnectorConfig.Database, ConfigType.String, Importance.High, "Database name")
        .Define(MySqlConnectorConfig.Username, ConfigType.String, Importance.High, "Username")
        .Define(MySqlConnectorConfig.Password, ConfigType.Password, Importance.High, "Password")
        .Define(MySqlConnectorConfig.Tables, ConfigType.List, Importance.High, "Tables to capture (database.table or table)")
        .Define(MySqlConnectorConfig.ServerId, ConfigType.Int, (int)MySqlConnectorConfig.DefaultServerId, Importance.Medium, "Unique server ID for binlog replication")
        .Define(MySqlConnectorConfig.TopicPrefix, ConfigType.String, "", Importance.Low, "Topic name prefix")
        .Define(MySqlConnectorConfig.TopicPattern, ConfigType.String, MySqlConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${database}, ${table})")
        .Define(MySqlConnectorConfig.SnapshotMode, ConfigType.String, MySqlConnectorConfig.SnapshotModeInitial, Importance.Medium, "Snapshot mode (initial, never, always, schema_only)", EditorHint.Select, options: ["initial", "never", "always", "schema_only"])
        .Define(MySqlConnectorConfig.IncludeSchema, ConfigType.Boolean, true, Importance.Low, "Include schema in topic name")
        .Define(MySqlConnectorConfig.IncludeBeforeValues, ConfigType.Boolean, true, Importance.Low, "Include before values in updates")
        .Define(MySqlConnectorConfig.SslMode, ConfigType.String, MySqlConnectorConfig.SslModeNone, Importance.Medium, "SSL mode", EditorHint.Select, options: ["none", "preferred", "required"])
        .Define(MySqlConnectorConfig.PollIntervalMs, ConfigType.Int, (int)MySqlConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in ms")
        .Define(MySqlConnectorConfig.BatchMaxRecords, ConfigType.Int, MySqlConnectorConfig.DefaultBatchMaxRecords, Importance.Low, "Max records per batch");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(MySqlConnectorConfig.Database, out var database) || string.IsNullOrEmpty(database))
            throw new ArgumentException($"Required configuration '{MySqlConnectorConfig.Database}' is missing");

        if (!config.TryGetValue(MySqlConnectorConfig.Username, out var username) || string.IsNullOrEmpty(username))
            throw new ArgumentException($"Required configuration '{MySqlConnectorConfig.Username}' is missing");

        if (!config.TryGetValue(MySqlConnectorConfig.Tables, out var tables) || string.IsNullOrEmpty(tables))
            throw new ArgumentException($"Required configuration '{MySqlConnectorConfig.Tables}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for CDC - binlog is sequential
        return [new Dictionary<string, string>(_config)];
    }
}
