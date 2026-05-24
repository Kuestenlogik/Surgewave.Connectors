namespace Kuestenlogik.Surgewave.Connector.Azure.Table;

/// <summary>
/// Configuration constants for Azure Table Storage connectors.
/// </summary>
public static class TableStorageConnectorConfig
{
    // Connection configs
    public const string ConnectionStringConfig = "azure.table.connection.string";
    public const string AccountNameConfig = "azure.table.account.name";
    public const string AccountKeyConfig = "azure.table.account.key";
    public const string EndpointConfig = "azure.table.endpoint";

    // Table configs
    public const string TableNameConfig = "azure.table.name";
    public const string TopicsConfig = "topics";

    // Source configs
    public const string QueryFilterConfig = "azure.table.query.filter";
    public const string SelectColumnsConfig = "azure.table.select.columns";
    public const string PollIntervalMsConfig = "azure.table.poll.interval.ms";
    public const string IncrementalModeConfig = "azure.table.incremental.mode";
    public const string IncrementalColumnConfig = "azure.table.incremental.column";
    public const string TopicPatternConfig = "azure.table.topic.pattern";
    public const string IncludeMetadataConfig = "azure.table.include.metadata";
    public const string MaxEntitiesPerPollConfig = "azure.table.max.entities.per.poll";

    // Sink configs
    public const string WriteModeConfig = "azure.table.write.mode";
    public const string BatchSizeConfig = "azure.table.batch.size";
    public const string PartitionKeyFieldConfig = "azure.table.partition.key.field";
    public const string RowKeyFieldConfig = "azure.table.row.key.field";
    public const string AutoCreateTableConfig = "azure.table.auto.create";
    public const string MaxRetryCountConfig = "azure.table.max.retry.count";
    public const string RetryDelayMsConfig = "azure.table.retry.delay.ms";

    // Offset tracking keys
    public const string OffsetTimestamp = "timestamp";
    public const string OffsetPartitionKey = "partition_key";
    public const string OffsetRowKey = "row_key";

    // Default values
    public const long DefaultPollIntervalMs = 5000;
    public const int DefaultBatchSize = 100;
    public const int DefaultMaxEntitiesPerPoll = 1000;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;
    public const string DefaultTopicPattern = "table.${table}";
    public const string DefaultIncrementalColumn = "Timestamp";

    // Write modes
    public const string WriteModeUpsert = "upsert";
    public const string WriteModeInsert = "insert";
    public const string WriteModeUpdate = "update";
    public const string WriteModeDelete = "delete";

    // Incremental modes
    public const string IncrementalModeNone = "none";
    public const string IncrementalModeTimestamp = "timestamp";
    public const string IncrementalModeRowKey = "rowkey";

    // Header names
    public const string HeaderTableName = "table.name";
    public const string HeaderPartitionKey = "table.partition.key";
    public const string HeaderRowKey = "table.row.key";
    public const string HeaderTimestamp = "table.timestamp";
    public const string HeaderEtag = "table.etag";
}
