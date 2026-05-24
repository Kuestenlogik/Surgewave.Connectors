namespace Kuestenlogik.Surgewave.Connector.Gcp.Bigtable;

/// <summary>
/// Configuration constants for Google Cloud Bigtable connector.
/// </summary>
public static class BigtableConnectorConfig
{
    // Connection settings
    public const string ProjectId = "gcp.project.id";
    public const string InstanceId = "bigtable.instance.id";
    public const string TableId = "bigtable.table.id";
    public const string CredentialsJson = "gcp.credentials.json";
    public const string CredentialsFile = "gcp.credentials.file";
    public const string EmulatorHost = "bigtable.emulator.host";

    // Source settings
    public const string Topic = "topic";
    public const string PollIntervalMs = "poll.interval.ms";
    public const string RowKeyPrefix = "bigtable.rowkey.prefix";
    public const string RowKeyStart = "bigtable.rowkey.start";
    public const string RowKeyEnd = "bigtable.rowkey.end";
    public const string ColumnFamily = "bigtable.column.family";
    public const string Columns = "bigtable.columns";  // Comma-separated column qualifiers
    public const string RowLimit = "bigtable.row.limit";
    public const string IncludeTimestamp = "bigtable.include.timestamp";

    // Sink settings
    public const string Topics = "topics";
    public const string RowKeyField = "bigtable.rowkey.field";
    public const string DefaultColumnFamily = "bigtable.default.column.family";
    public const string WriteMode = "bigtable.write.mode";  // set, append, increment
    public const string BatchSize = "bigtable.batch.size";

    // Defaults
    public const int DefaultPollIntervalMs = 5000;
    public const int DefaultRowLimit = 1000;
    public const int DefaultBatchSize = 100;
    public const string DefaultWriteMode = "set";
    public const string DefaultColumnFamilyName = "cf";
}
