namespace Kuestenlogik.Surgewave.Connector.Oracle;

/// <summary>
/// Configuration constants for Oracle CDC connectors using LogMiner.
/// </summary>
public static class OracleConnectorConfig
{
    // Connection settings
    public const string ConnectionString = "oracle.connection.string";
    public const string Host = "oracle.host";
    public const string Port = "oracle.port";
    public const string ServiceName = "oracle.service.name";
    public const string Sid = "oracle.sid";
    public const string Username = "oracle.username";
    public const string Password = "oracle.password";
    public const string WalletLocation = "oracle.wallet.location";

    // CDC settings
    public const string Tables = "oracle.tables";
    public const string TopicPrefix = "oracle.topic.prefix";
    public const string TopicPattern = "oracle.topic.pattern";
    public const string IncludeSchema = "oracle.include.schema";
    public const string IncludeBeforeValues = "oracle.include.before.values";

    // Snapshot settings
    public const string SnapshotMode = "oracle.snapshot.mode";

    // Polling settings
    public const string PollIntervalMs = "oracle.poll.interval.ms";
    public const string BatchMaxRecords = "oracle.batch.max.records";

    // LogMiner settings
    public const string LogMinerMode = "oracle.logminer.mode";
    public const string StartFromBeginning = "oracle.start.from.beginning";
    public const string DictionaryMode = "oracle.dictionary.mode";
    public const string SupplementalLogging = "oracle.supplemental.logging";

    // Offset tracking keys
    public const string OffsetScn = "scn";
    public const string OffsetCommitScn = "commit_scn";
    public const string OffsetSnapshotCompleted = "snapshot.completed";
    public const string OffsetTimestamp = "timestamp";

    // Default values
    public const string DefaultHost = "localhost";
    public const int DefaultPort = 1521;
    public const string DefaultTopicPattern = "${owner}.${table}";
    public const long DefaultPollIntervalMs = 500;
    public const int DefaultBatchMaxRecords = 1000;

    // Snapshot modes
    public const string SnapshotModeInitial = "initial";
    public const string SnapshotModeNever = "never";
    public const string SnapshotModeAlways = "always";
    public const string SnapshotModeSchemaOnly = "schema_only";

    // LogMiner modes
    public const string LogMinerModeOnline = "online";
    public const string LogMinerModeArchived = "archived";

    // Dictionary modes
    public const string DictionaryModeOnline = "online";
    public const string DictionaryModeRedoLog = "redo_log";

    // LogMiner operation types (from V$LOGMNR_CONTENTS.OPERATION_CODE)
    public const int OperationInsert = 1;
    public const int OperationDelete = 2;
    public const int OperationUpdate = 3;
    public const int OperationDdl = 5;
    public const int OperationStart = 6;
    public const int OperationCommit = 7;
    public const int OperationRollback = 36;

    // Header names
    public const string HeaderSchema = "oracle.owner";
    public const string HeaderTable = "oracle.table";
    public const string HeaderOperation = "oracle.op";
    public const string HeaderScn = "oracle.scn";
}
