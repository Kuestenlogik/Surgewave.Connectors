namespace Kuestenlogik.Surgewave.Connector.TimescaleDB;

/// <summary>
/// Configuration constants for TimescaleDB connector.
/// </summary>
public static class TimescaleConnectorConfig
{
    // Connection settings
    public const string ConnectionString = "timescale.connection.string";
    public const string Host = "timescale.host";
    public const string Port = "timescale.port";
    public const string Database = "timescale.database";
    public const string Username = "timescale.username";
    public const string Password = "timescale.password";
    public const string SslMode = "timescale.ssl.mode";

    // Source settings
    public const string Topic = "topic";
    public const string Query = "timescale.query";  // Custom SQL query
    public const string Table = "timescale.table";  // Hypertable name
    public const string TimeColumn = "timescale.time.column";  // Time column for incremental reads
    public const string Columns = "timescale.columns";  // Columns to select
    public const string PollIntervalMs = "poll.interval.ms";
    public const string LookbackSeconds = "timescale.lookback.seconds";  // How far back to look on first poll
    public const string RowLimit = "timescale.row.limit";

    // Sink settings
    public const string Topics = "topics";
    public const string TargetTable = "timescale.target.table";
    public const string TimeColumnField = "timescale.time.field";  // JSON field for time column
    public const string BatchSize = "timescale.batch.size";
    public const string InsertMode = "timescale.insert.mode";  // insert, upsert
    public const string ConflictColumns = "timescale.conflict.columns";  // For upsert

    // Defaults
    public const int DefaultPort = 5432;
    public const int DefaultPollIntervalMs = 1000;
    public const int DefaultLookbackSeconds = 3600;  // 1 hour
    public const int DefaultRowLimit = 10000;
    public const int DefaultBatchSize = 1000;
    public const string DefaultInsertMode = "insert";
    public const string DefaultSslMode = "prefer";
}
