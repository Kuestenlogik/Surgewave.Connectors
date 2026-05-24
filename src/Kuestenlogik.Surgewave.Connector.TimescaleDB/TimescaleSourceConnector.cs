using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.TimescaleDB;

/// <summary>
/// Source connector that reads from TimescaleDB hypertables.
/// </summary>
[ConnectorMetadata(
    Name = "timescale-source",
    Description = "Reads time-series data from TimescaleDB hypertables",
    Author = "Surgewave",
    Tags = "timescale, timescaledb, time-series, postgresql, sql, source")]
public sealed class TimescaleSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(TimescaleConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce rows to", EditorHint.Topic)
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
        .Define(TimescaleConnectorConfig.Query, ConfigType.String, "", Importance.Medium,
            "Custom SQL query (alternative to table)")
        .Define(TimescaleConnectorConfig.Table, ConfigType.String, "", Importance.Medium,
            "Hypertable name (alternative to query)")
        .Define(TimescaleConnectorConfig.TimeColumn, ConfigType.String, "time", Importance.High,
            "Time column for incremental reads")
        .Define(TimescaleConnectorConfig.Columns, ConfigType.List, "", Importance.Medium,
            "Columns to select (comma-separated, empty = all)")
        .Define(TimescaleConnectorConfig.PollIntervalMs, ConfigType.Int,
            TimescaleConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(TimescaleConnectorConfig.LookbackSeconds, ConfigType.Int,
            TimescaleConnectorConfig.DefaultLookbackSeconds.ToString(), Importance.Medium,
            "Seconds to look back on first poll")
        .Define(TimescaleConnectorConfig.RowLimit, ConfigType.Int,
            TimescaleConnectorConfig.DefaultRowLimit.ToString(), Importance.Medium,
            "Maximum rows per poll");

    public override Type TaskClass => typeof(TimescaleSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(TimescaleConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{TimescaleConnectorConfig.Topic}' is required");
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

        // Either query or table must be specified
        var query = config.GetValueOrDefault(TimescaleConnectorConfig.Query, "");
        var table = config.GetValueOrDefault(TimescaleConnectorConfig.Table, "");
        if (string.IsNullOrWhiteSpace(query) && string.IsNullOrWhiteSpace(table))
        {
            throw new ArgumentException($"Either '{TimescaleConnectorConfig.Query}' or '{TimescaleConnectorConfig.Table}' is required");
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
