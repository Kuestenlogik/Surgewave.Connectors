namespace Kuestenlogik.Surgewave.Connector.MySql;

/// <summary>
/// Configuration constants for MySQL CDC connector.
/// </summary>
public static class MySqlConnectorConfig
{
    // Connection settings
    public const string Host = "mysql.host";
    public const string Port = "mysql.port";
    public const string Database = "mysql.database";
    public const string Username = "mysql.username";
    public const string Password = "mysql.password";

    // SSL settings
    public const string SslMode = "mysql.ssl.mode";
    public const string SslCa = "mysql.ssl.ca";
    public const string SslCert = "mysql.ssl.cert";
    public const string SslKey = "mysql.ssl.key";

    // CDC settings
    public const string ServerId = "mysql.server.id";
    public const string Tables = "mysql.tables";
    public const string TopicPrefix = "mysql.topic.prefix";
    public const string TopicPattern = "mysql.topic.pattern";
    public const string IncludeSchema = "mysql.include.schema";
    public const string IncludeBeforeValues = "mysql.include.before.values";

    // Snapshot settings
    public const string SnapshotMode = "mysql.snapshot.mode";
    public const string SnapshotLockingMode = "mysql.snapshot.locking.mode";

    // Binlog settings
    public const string BinlogFilename = "mysql.binlog.filename";
    public const string BinlogPosition = "mysql.binlog.position";
    public const string GtidSet = "mysql.gtid.set";

    // Polling settings
    public const string PollIntervalMs = "mysql.poll.interval.ms";
    public const string BatchMaxRecords = "mysql.batch.max.records";

    // Offset tracking keys
    public const string OffsetBinlogFilename = "binlog.filename";
    public const string OffsetBinlogPosition = "binlog.position";
    public const string OffsetGtid = "gtid";
    public const string OffsetSnapshotCompleted = "snapshot.completed";
    public const string OffsetTimestamp = "timestamp";

    // Defaults
    public const string DefaultHost = "localhost";
    public const int DefaultPort = 3306;
    public const uint DefaultServerId = 65535;
    public const string DefaultTopicPattern = "${database}.${table}";
    public const long DefaultPollIntervalMs = 100;
    public const int DefaultBatchMaxRecords = 1000;

    // Snapshot modes
    public const string SnapshotModeInitial = "initial";
    public const string SnapshotModeNever = "never";
    public const string SnapshotModeAlways = "always";
    public const string SnapshotModeSchemaOnly = "schema_only";

    // SSL modes
    public const string SslModeNone = "none";
    public const string SslModePreferred = "preferred";
    public const string SslModeRequired = "required";
    public const string SslModeVerifyCa = "verify_ca";
    public const string SslModeVerifyFull = "verify_full";
}
