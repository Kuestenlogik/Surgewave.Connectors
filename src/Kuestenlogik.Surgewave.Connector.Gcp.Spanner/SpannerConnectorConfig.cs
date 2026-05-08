namespace Kuestenlogik.Surgewave.Connector.Gcp.Spanner;

/// <summary>
/// Configuration constants for Google Cloud Spanner connector.
/// </summary>
public static class SpannerConnectorConfig
{
    // Connection settings
    public const string ProjectId = "gcp.project.id";
    public const string InstanceId = "spanner.instance.id";
    public const string DatabaseId = "spanner.database.id";
    public const string CredentialsJson = "gcp.credentials.json";
    public const string CredentialsFile = "gcp.credentials.file";
    public const string EmulatorHost = "spanner.emulator.host";

    // Source settings
    public const string Topic = "topic";
    public const string Query = "spanner.query";  // SQL query to execute
    public const string Table = "spanner.table";  // Alternative: table to read
    public const string Columns = "spanner.columns";  // Columns to read (comma-separated)
    public const string PollIntervalMs = "poll.interval.ms";
    public const string IncrementalColumn = "spanner.incremental.column";  // Column for incremental reads
    public const string TimestampBound = "spanner.timestamp.bound";  // exact, strong, bounded_staleness
    public const string MaxStalenessSeconds = "spanner.max.staleness.seconds";
    public const string RowLimit = "spanner.row.limit";

    // Sink settings
    public const string Topics = "topics";
    public const string TargetTable = "spanner.target.table";
    public const string WriteMode = "spanner.write.mode";  // insert, update, upsert, delete
    public const string KeyColumns = "spanner.key.columns";  // Primary key columns (comma-separated)
    public const string BatchSize = "spanner.batch.size";
    public const string CommitTimeout = "spanner.commit.timeout.seconds";

    // Defaults
    public const int DefaultPollIntervalMs = 5000;
    public const int DefaultRowLimit = 1000;
    public const int DefaultBatchSize = 100;
    public const string DefaultWriteMode = "upsert";
    public const string DefaultTimestampBound = "strong";
    public const int DefaultMaxStalenessSeconds = 10;
    public const int DefaultCommitTimeoutSeconds = 60;
}
