using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.TimescaleDB;

/// <summary>
/// Sink connector that writes to TimescaleDB hypertables.
/// </summary>
[ConnectorMetadata(
    Name = "timescale-sink",
    Description = "Writes time-series data to TimescaleDB hypertables",
    Author = "Surgewave",
    Tags = "timescale, timescaledb, time-series, postgresql, sql, sink")]
public sealed class TimescaleSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(TimescaleConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(TimescaleConnectorConfig.ConnectionString, ConfigType.Password, "", Importance.High,
            "PostgreSQL connection string (alternative to individual settings)")
        .Define(TimescaleConnectorConfig.Host, ConfigType.String, "localhost", Importance.High,
            "TimescaleDB host")
        .Define(TimescaleConnectorConfig.Port, ConfigType.Int,
            TimescaleConnectorConfig.DefaultPort.ToString(), Importance.Medium,
            "TimescaleDB port")
        .Define(TimescaleConnectorConfig.Database, ConfigType.String, "", Importance.High,
            "Database name")
        .Define(TimescaleConnectorConfig.Username, ConfigType.String, "", Importance.High,
            "Username")
        .Define(TimescaleConnectorConfig.Password, ConfigType.Password, "", Importance.High,
            "Password")
        .Define(TimescaleConnectorConfig.SslMode, ConfigType.String,
            TimescaleConnectorConfig.DefaultSslMode, Importance.Low,
            "SSL mode: disable, prefer, require")
        .Define(TimescaleConnectorConfig.TargetTable, ConfigType.String, Importance.High,
            "Target hypertable for writes")
        .Define(TimescaleConnectorConfig.TimeColumnField, ConfigType.String, "time", Importance.Medium,
            "JSON field for time column")
        .Define(TimescaleConnectorConfig.BatchSize, ConfigType.Int,
            TimescaleConnectorConfig.DefaultBatchSize.ToString(), Importance.Medium,
            "Batch size for inserts")
        .Define(TimescaleConnectorConfig.InsertMode, ConfigType.String,
            TimescaleConnectorConfig.DefaultInsertMode, Importance.Medium,
            "Insert mode: insert, upsert")
        .Define(TimescaleConnectorConfig.ConflictColumns, ConfigType.List, "", Importance.Medium,
            "Conflict columns for upsert (comma-separated)");

    public override Type TaskClass => typeof(TimescaleSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(TimescaleConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{TimescaleConnectorConfig.Topics}' is required");
        }

        // Either connection string or individual settings
        var connStr = config.GetValueOrDefault(TimescaleConnectorConfig.ConnectionString, "");
        if (string.IsNullOrWhiteSpace(connStr))
        {
            if (!config.TryGetValue(TimescaleConnectorConfig.Database, out var db) ||
                string.IsNullOrWhiteSpace(db))
            {
                throw new ArgumentException($"'{TimescaleConnectorConfig.Database}' is required when not using connection string");
            }
        }

        if (!config.TryGetValue(TimescaleConnectorConfig.TargetTable, out var table) ||
            string.IsNullOrWhiteSpace(table))
        {
            throw new ArgumentException($"'{TimescaleConnectorConfig.TargetTable}' is required");
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
