using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Sap.Hana;

/// <summary>
/// Sink connector that writes to SAP HANA database.
/// </summary>
[ConnectorMetadata(
    Name = "sap-hana-sink",
    Description = "Writes data to SAP HANA in-memory database",
    Author = "Surgewave",
    Tags = "sap, hana, database, in-memory, sql, sink")]
public sealed class HanaSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(HanaConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(HanaConnectorConfig.ConnectionString, ConfigType.Password, "", Importance.High,
            "HANA connection string (alternative to individual settings)")
        .Define(HanaConnectorConfig.Host, ConfigType.String, "", Importance.High,
            "HANA server host")
        .Define(HanaConnectorConfig.Port, ConfigType.Int,
            HanaConnectorConfig.DefaultPort.ToString(), Importance.Medium,
            "HANA server port")
        .Define(HanaConnectorConfig.Database, ConfigType.String, "", Importance.Medium,
            "Database name (for multi-tenant)")
        .Define(HanaConnectorConfig.Schema, ConfigType.String, "", Importance.Medium,
            "Schema name")
        .Define(HanaConnectorConfig.Username, ConfigType.String, "", Importance.High,
            "Username")
        .Define(HanaConnectorConfig.Password, ConfigType.Password, "", Importance.High,
            "Password")
        .Define(HanaConnectorConfig.UseSsl, ConfigType.Boolean, "true", Importance.Medium,
            "Enable SSL/TLS")
        .Define(HanaConnectorConfig.TargetTable, ConfigType.String, Importance.High,
            "Target table for writes")
        .Define(HanaConnectorConfig.WriteMode, ConfigType.String,
            HanaConnectorConfig.DefaultWriteMode, Importance.Medium,
            "Write mode: insert, upsert, merge", EditorHint.Select, options: ["insert", "upsert", "update"])
        .Define(HanaConnectorConfig.KeyColumns, ConfigType.List, "", Importance.Medium,
            "Key columns for upsert/merge (comma-separated)")
        .Define(HanaConnectorConfig.BatchSize, ConfigType.Int,
            HanaConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Batch size for inserts");

    public override Type TaskClass => typeof(HanaSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(HanaConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{HanaConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(HanaConnectorConfig.TargetTable, out var table) ||
            string.IsNullOrWhiteSpace(table))
        {
            throw new ArgumentException($"'{HanaConnectorConfig.TargetTable}' is required");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
