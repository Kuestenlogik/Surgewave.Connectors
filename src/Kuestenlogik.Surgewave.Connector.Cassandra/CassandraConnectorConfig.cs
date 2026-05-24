namespace Kuestenlogik.Surgewave.Connector.Cassandra;

/// <summary>
/// Configuration constants for Apache Cassandra connectors.
/// </summary>
public static class CassandraConnectorConfig
{
    // Connection configs
    public const string ContactPointsConfig = "cassandra.contact.points";
    public const string PortConfig = "cassandra.port";
    public const string DatacenterConfig = "cassandra.datacenter";
    public const string KeyspaceConfig = "cassandra.keyspace";
    public const string UsernameConfig = "cassandra.username";
    public const string PasswordConfig = "cassandra.password";
    public const string ConsistencyLevelConfig = "cassandra.consistency";
    public const string SslEnabledConfig = "cassandra.ssl.enabled";

    // Table configs
    public const string TableConfig = "cassandra.table";

    // Source configs
    public const string TopicPatternConfig = "cassandra.topic.pattern";
    public const string PollIntervalMsConfig = "cassandra.poll.interval.ms";
    public const string MaxRowsPerPollConfig = "cassandra.max.rows.per.poll";
    public const string IncludeMetadataConfig = "cassandra.include.metadata";
    public const string ModeConfig = "cassandra.mode"; // table, query
    public const string QueryConfig = "cassandra.query";
    public const string TimestampColumnConfig = "cassandra.timestamp.column";
    public const string PartitionKeyColumnsConfig = "cassandra.partition.key.columns";
    public const string ClusteringKeyColumnsConfig = "cassandra.clustering.key.columns";

    // Sink configs
    public const string TopicsConfig = "topics";
    public const string WriteModeConfig = "cassandra.write.mode"; // insert, upsert
    public const string BatchSizeConfig = "cassandra.batch.size";
    public const string MaxRetryCountConfig = "cassandra.max.retry.count";
    public const string RetryDelayMsConfig = "cassandra.retry.delay.ms";
    public const string BatchTypeConfig = "cassandra.batch.type"; // logged, unlogged, counter
    public const string TtlSecondsConfig = "cassandra.ttl.seconds";

    // Default values
    public const string DefaultTopicPattern = "cassandra.${keyspace}.${table}";
    public const int DefaultPort = 9042;
    public const long DefaultPollIntervalMs = 5000;
    public const int DefaultMaxRowsPerPoll = 10000;
    public const string DefaultWriteMode = "insert";
    public const string DefaultMode = "table";
    public const string DefaultConsistencyLevel = "LOCAL_QUORUM";
    public const int DefaultBatchSize = 500;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;
    public const string DefaultBatchType = "unlogged";
    public const int DefaultTtlSeconds = 0; // No TTL

    // Header names
    public const string HeaderKeyspace = "cassandra.keyspace";
    public const string HeaderTable = "cassandra.table";
    public const string HeaderPartitionKey = "cassandra.partition.key";
    public const string HeaderClusteringKey = "cassandra.clustering.key";
    public const string HeaderWriteTime = "cassandra.writetime";
    public const string HeaderTtl = "cassandra.ttl";
    public const string HeaderTimestamp = "cassandra.timestamp";

    // Offset tracking
    public const string OffsetTimestamp = "timestamp";
    public const string OffsetPartitionKey = "partition_key";
    public const string OffsetClusteringKey = "clustering_key";
    public const string OffsetTable = "table";
}
