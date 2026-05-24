using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;

namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb.Tests;

public class CosmosDbConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("azure.cosmosdb.connection.string", CosmosDbConnectorConfig.ConnectionStringConfig);
        Assert.Equal("azure.cosmosdb.endpoint", CosmosDbConnectorConfig.EndpointConfig);
        Assert.Equal("azure.cosmosdb.account.key", CosmosDbConnectorConfig.AccountKeyConfig);
    }

    [Fact]
    public void DatabaseSettings_HaveExpectedValues()
    {
        Assert.Equal("azure.cosmosdb.database", CosmosDbConnectorConfig.DatabaseConfig);
        Assert.Equal("azure.cosmosdb.container", CosmosDbConnectorConfig.ContainerConfig);
        Assert.Equal("topics", CosmosDbConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void ChangeFeedSettings_HaveExpectedValues()
    {
        Assert.Equal("azure.cosmosdb.changefeed.start.from", CosmosDbConnectorConfig.ChangeFeedStartFromConfig);
        Assert.Equal("azure.cosmosdb.changefeed.max.items", CosmosDbConnectorConfig.ChangeFeedMaxItemsConfig);
        Assert.Equal("azure.cosmosdb.changefeed.poll.interval.ms", CosmosDbConnectorConfig.ChangeFeedPollIntervalMsConfig);
        Assert.Equal("azure.cosmosdb.lease.container", CosmosDbConnectorConfig.LeaseContainerConfig);
        Assert.Equal("azure.cosmosdb.lease.prefix", CosmosDbConnectorConfig.LeaseContainerPrefixConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("azure.cosmosdb.write.mode", CosmosDbConnectorConfig.WriteModeConfig);
        Assert.Equal("azure.cosmosdb.partition.key.path", CosmosDbConnectorConfig.PartitionKeyPathConfig);
        Assert.Equal("azure.cosmosdb.id.field", CosmosDbConnectorConfig.IdFieldConfig);
        Assert.Equal("azure.cosmosdb.batch.size", CosmosDbConnectorConfig.BatchSizeConfig);
        Assert.Equal("azure.cosmosdb.auto.create.container", CosmosDbConnectorConfig.AutoCreateContainerConfig);
        Assert.Equal("azure.cosmosdb.throughput", CosmosDbConnectorConfig.ThroughputConfig);
        Assert.Equal("azure.cosmosdb.max.retry.count", CosmosDbConnectorConfig.MaxRetryCountConfig);
        Assert.Equal("azure.cosmosdb.max.retry.wait.time.ms", CosmosDbConnectorConfig.MaxRetryWaitTimeMsConfig);
    }

    [Fact]
    public void TopicPatternSettings_HaveExpectedValues()
    {
        Assert.Equal("azure.cosmosdb.topic.pattern", CosmosDbConnectorConfig.TopicPatternConfig);
        Assert.Equal("azure.cosmosdb.include.metadata", CosmosDbConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("continuation_token", CosmosDbConnectorConfig.OffsetContinuationToken);
        Assert.Equal("partition_key", CosmosDbConnectorConfig.OffsetPartitionKey);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal(100, CosmosDbConnectorConfig.DefaultChangeFeedMaxItems);
        Assert.Equal(500L, CosmosDbConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(100, CosmosDbConnectorConfig.DefaultBatchSize);
        Assert.Equal(400, CosmosDbConnectorConfig.DefaultThroughput);
        Assert.Equal(9, CosmosDbConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(30000L, CosmosDbConnectorConfig.DefaultMaxRetryWaitTimeMs);
        Assert.Equal("cosmosdb.${database}.${container}", CosmosDbConnectorConfig.DefaultTopicPattern);
        Assert.Equal("surgewave-connector", CosmosDbConnectorConfig.DefaultLeaseContainerPrefix);
    }

    [Fact]
    public void WriteModes_HaveExpectedValues()
    {
        Assert.Equal("upsert", CosmosDbConnectorConfig.WriteModeUpsert);
        Assert.Equal("create", CosmosDbConnectorConfig.WriteModeCreate);
        Assert.Equal("replace", CosmosDbConnectorConfig.WriteModeReplace);
        Assert.Equal("delete", CosmosDbConnectorConfig.WriteModeDelete);
    }

    [Fact]
    public void StartFromOptions_HaveExpectedValues()
    {
        Assert.Equal("beginning", CosmosDbConnectorConfig.StartFromBeginning);
        Assert.Equal("now", CosmosDbConnectorConfig.StartFromNow);
        Assert.Equal("continuation", CosmosDbConnectorConfig.StartFromContinuation);
    }

    [Fact]
    public void OperationTypes_HaveExpectedValues()
    {
        Assert.Equal("create", CosmosDbConnectorConfig.OperationCreate);
        Assert.Equal("replace", CosmosDbConnectorConfig.OperationReplace);
        Assert.Equal("delete", CosmosDbConnectorConfig.OperationDelete);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("cosmosdb.database", CosmosDbConnectorConfig.HeaderDatabase);
        Assert.Equal("cosmosdb.container", CosmosDbConnectorConfig.HeaderContainer);
        Assert.Equal("cosmosdb.partition.key", CosmosDbConnectorConfig.HeaderPartitionKey);
        Assert.Equal("cosmosdb.etag", CosmosDbConnectorConfig.HeaderEtag);
        Assert.Equal("cosmosdb.timestamp", CosmosDbConnectorConfig.HeaderTimestamp);
        Assert.Equal("cosmosdb.lsn", CosmosDbConnectorConfig.HeaderLsn);
    }
}
