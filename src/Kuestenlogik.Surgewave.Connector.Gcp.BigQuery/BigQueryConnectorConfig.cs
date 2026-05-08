namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;

/// <summary>
/// Configuration constants for BigQuery connectors.
/// </summary>
public static class BigQueryConnectorConfig
{
    // Connection configs
    public const string ProjectIdConfig = "gcp.bigquery.project.id";
    public const string CredentialsJsonConfig = "gcp.bigquery.credentials.json";
    public const string CredentialsFileConfig = "gcp.bigquery.credentials.file";
    public const string DatasetConfig = "gcp.bigquery.dataset";
    public const string LocationConfig = "gcp.bigquery.location";

    // Source configs
    public const string TableConfig = "gcp.bigquery.table";
    public const string QueryConfig = "gcp.bigquery.query";
    public const string TopicPatternConfig = "gcp.bigquery.topic.pattern";
    public const string PollIntervalMsConfig = "gcp.bigquery.poll.interval.ms";
    public const string MaxRowsPerPollConfig = "gcp.bigquery.max.rows.per.poll";
    public const string IncludeMetadataConfig = "gcp.bigquery.include.metadata";
    public const string ModeConfig = "gcp.bigquery.mode"; // table, query
    public const string TimestampColumnConfig = "gcp.bigquery.timestamp.column";
    public const string PartitionFieldConfig = "gcp.bigquery.partition.field";
    public const string UseStandardSqlConfig = "gcp.bigquery.use.standard.sql";

    // Sink configs
    public const string TopicsConfig = "topics";
    public const string WriteModeConfig = "gcp.bigquery.write.mode"; // insert, append, truncate
    public const string BatchSizeConfig = "gcp.bigquery.batch.size";
    public const string MaxRetryCountConfig = "gcp.bigquery.max.retry.count";
    public const string RetryDelayMsConfig = "gcp.bigquery.retry.delay.ms";
    public const string AutoCreateTableConfig = "gcp.bigquery.auto.create.table";
    public const string AutoCreateDatasetConfig = "gcp.bigquery.auto.create.dataset";
    public const string UseStreamingConfig = "gcp.bigquery.use.streaming"; // streaming insert vs load job
    public const string SchemaUpdateOptionsConfig = "gcp.bigquery.schema.update.options";
    public const string TimePartitioningConfig = "gcp.bigquery.time.partitioning"; // DAY, HOUR, MONTH, YEAR
    public const string ClusteringFieldsConfig = "gcp.bigquery.clustering.fields";

    // Default values
    public const string DefaultTopicPattern = "bigquery.${project}.${dataset}.${table}";
    public const long DefaultPollIntervalMs = 60000; // 1 minute (BigQuery is for batch analytics)
    public const int DefaultMaxRowsPerPoll = 10000;
    public const string DefaultWriteMode = "append";
    public const int DefaultBatchSize = 10000;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;
    public const string DefaultMode = "table";
    public const string DefaultLocation = "US";

    // Header names
    public const string HeaderProjectId = "bigquery.project.id";
    public const string HeaderDataset = "bigquery.dataset";
    public const string HeaderTable = "bigquery.table";
    public const string HeaderLocation = "bigquery.location";
    public const string HeaderPartitionTime = "bigquery.partition.time";
    public const string HeaderInsertId = "bigquery.insert.id";
    public const string HeaderTimestamp = "bigquery.timestamp";

    // Offset tracking
    public const string OffsetTimestamp = "timestamp";
    public const string OffsetPartition = "partition";
    public const string OffsetRowNumber = "row_number";
    public const string OffsetTable = "table";
}
