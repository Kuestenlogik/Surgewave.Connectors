using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Oracle;

/// <summary>
/// Source connector that captures changes from Oracle using LogMiner.
/// Reads redo logs for INSERT, UPDATE, and DELETE events and produces Debezium-compatible output.
/// </summary>
public sealed class OracleCdcSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(OracleCdcSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(OracleConnectorConfig.ConnectionString, ConfigType.String, "", Importance.High, "Oracle connection string (alternative to individual settings)")
        .Define(OracleConnectorConfig.Host, ConfigType.String, OracleConnectorConfig.DefaultHost, Importance.High, "Oracle hostname")
        .Define(OracleConnectorConfig.Port, ConfigType.Int, OracleConnectorConfig.DefaultPort, Importance.High, "Oracle port")
        .Define(OracleConnectorConfig.ServiceName, ConfigType.String, "", Importance.High, "Oracle service name")
        .Define(OracleConnectorConfig.Sid, ConfigType.String, "", Importance.Medium, "Oracle SID (alternative to service name)")
        .Define(OracleConnectorConfig.Username, ConfigType.String, Importance.High, "Username")
        .Define(OracleConnectorConfig.Password, ConfigType.Password, Importance.High, "Password")
        .Define(OracleConnectorConfig.WalletLocation, ConfigType.String, "", Importance.Low, "Oracle wallet location for secure external password store", EditorHint.FilePath)
        .Define(OracleConnectorConfig.Tables, ConfigType.List, Importance.High, "Tables to capture (owner.table or table)")
        .Define(OracleConnectorConfig.TopicPrefix, ConfigType.String, "", Importance.Low, "Topic name prefix")
        .Define(OracleConnectorConfig.TopicPattern, ConfigType.String, OracleConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${owner}, ${table})")
        .Define(OracleConnectorConfig.SnapshotMode, ConfigType.String, OracleConnectorConfig.SnapshotModeInitial, Importance.Medium, "Snapshot mode (initial, never, always, schema_only)", EditorHint.Select, options: ["initial", "never", "always", "schema_only"])
        .Define(OracleConnectorConfig.IncludeSchema, ConfigType.Boolean, true, Importance.Low, "Include owner/schema in topic name")
        .Define(OracleConnectorConfig.IncludeBeforeValues, ConfigType.Boolean, true, Importance.Low, "Include before values in updates")
        .Define(OracleConnectorConfig.PollIntervalMs, ConfigType.Int, (int)OracleConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in ms")
        .Define(OracleConnectorConfig.BatchMaxRecords, ConfigType.Int, OracleConnectorConfig.DefaultBatchMaxRecords, Importance.Low, "Max records per batch")
        .Define(OracleConnectorConfig.LogMinerMode, ConfigType.String, OracleConnectorConfig.LogMinerModeOnline, Importance.Medium, "LogMiner mode (online, archived)", EditorHint.Select, options: ["online", "archived"])
        .Define(OracleConnectorConfig.DictionaryMode, ConfigType.String, OracleConnectorConfig.DictionaryModeOnline, Importance.Low, "Dictionary mode (online, redo_log)", EditorHint.Select, options: ["online", "redo_log"])
        .Define(OracleConnectorConfig.StartFromBeginning, ConfigType.Boolean, false, Importance.Medium, "Start from beginning of available redo logs");

    public override void Start(IDictionary<string, string> config)
    {
        // Require either connection string or service name/SID
        var hasConnectionString = config.TryGetValue(OracleConnectorConfig.ConnectionString, out var connStr) && !string.IsNullOrEmpty(connStr);
        var hasServiceName = config.TryGetValue(OracleConnectorConfig.ServiceName, out var serviceName) && !string.IsNullOrEmpty(serviceName);
        var hasSid = config.TryGetValue(OracleConnectorConfig.Sid, out var sid) && !string.IsNullOrEmpty(sid);

        if (!hasConnectionString && !hasServiceName && !hasSid)
            throw new ArgumentException($"Required configuration '{OracleConnectorConfig.ServiceName}', '{OracleConnectorConfig.Sid}', or '{OracleConnectorConfig.ConnectionString}' is missing");

        if (!config.TryGetValue(OracleConnectorConfig.Tables, out var tables) || string.IsNullOrEmpty(tables))
            throw new ArgumentException($"Required configuration '{OracleConnectorConfig.Tables}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for CDC - LogMiner polling is sequential
        return [new Dictionary<string, string>(_config)];
    }
}
