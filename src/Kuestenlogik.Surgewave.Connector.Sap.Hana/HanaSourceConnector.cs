using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Sap.Hana;

/// <summary>
/// Source connector that reads from SAP HANA database.
/// </summary>
[ConnectorMetadata(
    Name = "sap-hana-source",
    Description = "Reads data from SAP HANA in-memory database",
    Author = "Surgewave",
    Tags = "sap, hana, database, in-memory, sql, source")]
public sealed class HanaSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(HanaConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce rows to", EditorHint.Topic)
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
        .Define(HanaConnectorConfig.ValidateCertificate, ConfigType.Boolean, "true", Importance.Low,
            "Validate server certificate")
        .Define(HanaConnectorConfig.Query, ConfigType.String, "", Importance.Medium,
            "SQL query (alternative to table)", EditorHint.Code, "sql")
        .Define(HanaConnectorConfig.Table, ConfigType.String, "", Importance.Medium,
            "Table name (alternative to query)")
        .Define(HanaConnectorConfig.Columns, ConfigType.List, "", Importance.Medium,
            "Columns to select (comma-separated)")
        .Define(HanaConnectorConfig.IncrementalColumn, ConfigType.String, "", Importance.Medium,
            "Column for incremental reads")
        .Define(HanaConnectorConfig.PollIntervalMs, ConfigType.Int,
            HanaConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(HanaConnectorConfig.RowLimit, ConfigType.Int,
            HanaConnectorConfig.DefaultRowLimit.ToString(), Importance.Medium,
            "Maximum rows per poll");

    public override Type TaskClass => typeof(HanaSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(HanaConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{HanaConnectorConfig.Topic}' is required");
        }

        var query = config.GetValueOrDefault(HanaConnectorConfig.Query, "");
        var table = config.GetValueOrDefault(HanaConnectorConfig.Table, "");
        if (string.IsNullOrWhiteSpace(query) && string.IsNullOrWhiteSpace(table))
        {
            throw new ArgumentException($"Either '{HanaConnectorConfig.Query}' or '{HanaConnectorConfig.Table}' is required");
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
