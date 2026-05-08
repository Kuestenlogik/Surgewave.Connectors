namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;

/// <summary>
/// Configuration constants for Azure Cosmos DB connectors.
/// </summary>
public static class CosmosDbConnectorConfig
{
    // Connection configs
    public const string ConnectionStringConfig = "azure.cosmosdb.connection.string";
    public const string EndpointConfig = "azure.cosmosdb.endpoint";
    public const string AccountKeyConfig = "azure.cosmosdb.account.key";

    // Database and container configs
    public const string DatabaseConfig = "azure.cosmosdb.database";
    public const string ContainerConfig = "azure.cosmosdb.container";
    public const string TopicsConfig = "topics";

    // Source (Change Feed) configs
    public const string ChangeFeedStartFromConfig = "azure.cosmosdb.changefeed.start.from";
    public const string ChangeFeedMaxItemsConfig = "azure.cosmosdb.changefeed.max.items";
    public const string ChangeFeedPollIntervalMsConfig = "azure.cosmosdb.changefeed.poll.interval.ms";
    public const string LeaseContainerConfig = "azure.cosmosdb.lease.container";
    public const string LeaseContainerPrefixConfig = "azure.cosmosdb.lease.prefix";

    // Sink configs
    public const string WriteModeConfig = "azure.cosmosdb.write.mode";
    public const string PartitionKeyPathConfig = "azure.cosmosdb.partition.key.path";
    public const string IdFieldConfig = "azure.cosmosdb.id.field";
    public const string BatchSizeConfig = "azure.cosmosdb.batch.size";
    public const string AutoCreateContainerConfig = "azure.cosmosdb.auto.create.container";
    public const string ThroughputConfig = "azure.cosmosdb.throughput";
    public const string MaxRetryCountConfig = "azure.cosmosdb.max.retry.count";
    public const string MaxRetryWaitTimeMsConfig = "azure.cosmosdb.max.retry.wait.time.ms";

    // Topic pattern
    public const string TopicPatternConfig = "azure.cosmosdb.topic.pattern";
    public const string IncludeMetadataConfig = "azure.cosmosdb.include.metadata";

    // Offset tracking keys
    public const string OffsetContinuationToken = "continuation_token";
    public const string OffsetPartitionKey = "partition_key";

    // Default values
    public const int DefaultChangeFeedMaxItems = 100;
    public const long DefaultPollIntervalMs = 500;
    public const int DefaultBatchSize = 100;
    public const int DefaultThroughput = 400;
    public const int DefaultMaxRetryCount = 9;
    public const long DefaultMaxRetryWaitTimeMs = 30000;
    public const string DefaultTopicPattern = "cosmosdb.${database}.${container}";
    public const string DefaultLeaseContainerPrefix = "surgewave-connector";

    // Write modes
    public const string WriteModeUpsert = "upsert";
    public const string WriteModeCreate = "create";
    public const string WriteModeReplace = "replace";
    public const string WriteModeDelete = "delete";

    // Change feed start from options
    public const string StartFromBeginning = "beginning";
    public const string StartFromNow = "now";
    public const string StartFromContinuation = "continuation";

    // Change feed operation types
    public const string OperationCreate = "create";
    public const string OperationReplace = "replace";
    public const string OperationDelete = "delete";

    // Header names
    public const string HeaderDatabase = "cosmosdb.database";
    public const string HeaderContainer = "cosmosdb.container";
    public const string HeaderPartitionKey = "cosmosdb.partition.key";
    public const string HeaderEtag = "cosmosdb.etag";
    public const string HeaderTimestamp = "cosmosdb.timestamp";
    public const string HeaderLsn = "cosmosdb.lsn";
}
