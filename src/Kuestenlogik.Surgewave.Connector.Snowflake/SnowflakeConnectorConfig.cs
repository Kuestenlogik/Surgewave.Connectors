namespace Kuestenlogik.Surgewave.Connector.Snowflake;

/// <summary>
/// Configuration constants for Snowflake connectors.
/// </summary>
public static class SnowflakeConnectorConfig
{
    // Connection configs
    public const string AccountConfig = "snowflake.account";
    public const string UserConfig = "snowflake.user";
    public const string PasswordConfig = "snowflake.password";
    public const string DatabaseConfig = "snowflake.database";
    public const string SchemaConfig = "snowflake.schema";
    public const string WarehouseConfig = "snowflake.warehouse";
    public const string RoleConfig = "snowflake.role";
    public const string AuthenticatorConfig = "snowflake.authenticator"; // snowflake, externalbrowser, oauth, jwt
    public const string PrivateKeyFileConfig = "snowflake.private.key.file";
    public const string PrivateKeyPassphraseConfig = "snowflake.private.key.passphrase";
    public const string OAuthTokenConfig = "snowflake.oauth.token";

    // Source configs
    public const string TableConfig = "snowflake.table";
    public const string QueryConfig = "snowflake.query";
    public const string StreamNameConfig = "snowflake.stream.name";
    public const string TopicPatternConfig = "snowflake.topic.pattern";
    public const string PollIntervalMsConfig = "snowflake.poll.interval.ms";
    public const string MaxRowsPerPollConfig = "snowflake.max.rows.per.poll";
    public const string IncludeMetadataConfig = "snowflake.include.metadata";
    public const string ModeConfig = "snowflake.mode"; // table, query, stream
    public const string TimestampColumnConfig = "snowflake.timestamp.column";
    public const string IncrementingColumnConfig = "snowflake.incrementing.column";

    // Sink configs
    public const string TopicsConfig = "topics";
    public const string WriteModeConfig = "snowflake.write.mode"; // insert, upsert, merge
    public const string BatchSizeConfig = "snowflake.batch.size";
    public const string StageNameConfig = "snowflake.stage.name";
    public const string UseSnowpipeConfig = "snowflake.use.snowpipe";
    public const string PipeNameConfig = "snowflake.pipe.name";
    public const string MaxRetryCountConfig = "snowflake.max.retry.count";
    public const string RetryDelayMsConfig = "snowflake.retry.delay.ms";
    public const string KeyColumnsConfig = "snowflake.key.columns"; // comma-separated for upsert/merge
    public const string AutoCreateTableConfig = "snowflake.auto.create.table";

    // Default values
    public const string DefaultTopicPattern = "snowflake.${database}.${schema}.${table}";
    public const long DefaultPollIntervalMs = 5000;
    public const int DefaultMaxRowsPerPoll = 10000;
    public const string DefaultWriteMode = "insert";
    public const int DefaultBatchSize = 10000;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;
    public const string DefaultMode = "table";

    // Header names
    public const string HeaderAccount = "snowflake.account";
    public const string HeaderDatabase = "snowflake.database";
    public const string HeaderSchema = "snowflake.schema";
    public const string HeaderTable = "snowflake.table";
    public const string HeaderWarehouse = "snowflake.warehouse";
    public const string HeaderStreamName = "snowflake.stream.name";
    public const string HeaderActionType = "snowflake.action.type";
    public const string HeaderRowId = "snowflake.row.id";
    public const string HeaderTimestamp = "snowflake.timestamp";

    // Offset tracking
    public const string OffsetTimestamp = "timestamp";
    public const string OffsetIncrementingColumn = "incrementing_value";
    public const string OffsetStreamPosition = "stream_position";
    public const string OffsetTable = "table";

    // Stream metadata columns (CDC)
    public const string MetadataAction = "METADATA$ACTION";
    public const string MetadataIsUpdate = "METADATA$ISUPDATE";
    public const string MetadataRowId = "METADATA$ROW_ID";

    // Action types
    public const string ActionInsert = "INSERT";
    public const string ActionDelete = "DELETE";
}
