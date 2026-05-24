namespace Kuestenlogik.Surgewave.Connector.SqlServer;

/// <summary>
/// Configuration constants for SQL Server CDC connector.
/// </summary>
public static class SqlServerConnectorConfig
{
    // Connection settings
    public const string ConnectionString = "sqlserver.connection.string";
    public const string Server = "sqlserver.server";
    public const string Database = "sqlserver.database";
    public const string Username = "sqlserver.username";
    public const string Password = "sqlserver.password";
    public const string TrustServerCertificate = "sqlserver.trust.server.certificate";
    public const string Encrypt = "sqlserver.encrypt";

    // CDC settings
    public const string Tables = "sqlserver.tables";
    public const string TopicPrefix = "sqlserver.topic.prefix";
    public const string TopicPattern = "sqlserver.topic.pattern";
    public const string IncludeSchema = "sqlserver.include.schema";
    public const string IncludeBeforeValues = "sqlserver.include.before.values";

    // Snapshot settings
    public const string SnapshotMode = "sqlserver.snapshot.mode";

    // Polling settings
    public const string PollIntervalMs = "sqlserver.poll.interval.ms";
    public const string BatchMaxRecords = "sqlserver.batch.max.records";

    // CDC tracking settings
    public const string CaptureInstance = "sqlserver.capture.instance";
    public const string StartFromBeginning = "sqlserver.start.from.beginning";

    // Offset tracking keys
    public const string OffsetLsn = "lsn";
    public const string OffsetSeqVal = "seqval";
    public const string OffsetSnapshotCompleted = "snapshot.completed";
    public const string OffsetTimestamp = "timestamp";

    // Defaults
    public const string DefaultServer = "localhost";
    public const int DefaultPort = 1433;
    public const string DefaultTopicPattern = "${schema}.${table}";
    public const long DefaultPollIntervalMs = 500;
    public const int DefaultBatchMaxRecords = 1000;

    // Snapshot modes
    public const string SnapshotModeInitial = "initial";
    public const string SnapshotModeNever = "never";
    public const string SnapshotModeAlways = "always";
    public const string SnapshotModeSchemaOnly = "schema_only";

    // CDC Operation types (from __$operation column)
    public const int CdcOperationDelete = 1;
    public const int CdcOperationInsert = 2;
    public const int CdcOperationUpdateBefore = 3;
    public const int CdcOperationUpdateAfter = 4;
}
