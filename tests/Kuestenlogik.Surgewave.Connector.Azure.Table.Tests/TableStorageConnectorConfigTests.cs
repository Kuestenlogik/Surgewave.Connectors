using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.Table;

namespace Kuestenlogik.Surgewave.Connector.Azure.Table.Tests;

public class TableStorageConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("azure.table.connection.string", TableStorageConnectorConfig.ConnectionStringConfig);
        Assert.Equal("azure.table.account.name", TableStorageConnectorConfig.AccountNameConfig);
        Assert.Equal("azure.table.account.key", TableStorageConnectorConfig.AccountKeyConfig);
        Assert.Equal("azure.table.endpoint", TableStorageConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void TableSettings_HaveExpectedValues()
    {
        Assert.Equal("azure.table.name", TableStorageConnectorConfig.TableNameConfig);
        Assert.Equal("topics", TableStorageConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void SourceSettings_HaveExpectedValues()
    {
        Assert.Equal("azure.table.query.filter", TableStorageConnectorConfig.QueryFilterConfig);
        Assert.Equal("azure.table.select.columns", TableStorageConnectorConfig.SelectColumnsConfig);
        Assert.Equal("azure.table.poll.interval.ms", TableStorageConnectorConfig.PollIntervalMsConfig);
        Assert.Equal("azure.table.incremental.mode", TableStorageConnectorConfig.IncrementalModeConfig);
        Assert.Equal("azure.table.incremental.column", TableStorageConnectorConfig.IncrementalColumnConfig);
        Assert.Equal("azure.table.topic.pattern", TableStorageConnectorConfig.TopicPatternConfig);
        Assert.Equal("azure.table.include.metadata", TableStorageConnectorConfig.IncludeMetadataConfig);
        Assert.Equal("azure.table.max.entities.per.poll", TableStorageConnectorConfig.MaxEntitiesPerPollConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("azure.table.write.mode", TableStorageConnectorConfig.WriteModeConfig);
        Assert.Equal("azure.table.batch.size", TableStorageConnectorConfig.BatchSizeConfig);
        Assert.Equal("azure.table.partition.key.field", TableStorageConnectorConfig.PartitionKeyFieldConfig);
        Assert.Equal("azure.table.row.key.field", TableStorageConnectorConfig.RowKeyFieldConfig);
        Assert.Equal("azure.table.auto.create", TableStorageConnectorConfig.AutoCreateTableConfig);
        Assert.Equal("azure.table.max.retry.count", TableStorageConnectorConfig.MaxRetryCountConfig);
        Assert.Equal("azure.table.retry.delay.ms", TableStorageConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("timestamp", TableStorageConnectorConfig.OffsetTimestamp);
        Assert.Equal("partition_key", TableStorageConnectorConfig.OffsetPartitionKey);
        Assert.Equal("row_key", TableStorageConnectorConfig.OffsetRowKey);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal(5000L, TableStorageConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(100, TableStorageConnectorConfig.DefaultBatchSize);
        Assert.Equal(1000, TableStorageConnectorConfig.DefaultMaxEntitiesPerPoll);
        Assert.Equal(3, TableStorageConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, TableStorageConnectorConfig.DefaultRetryDelayMs);
        Assert.Equal("table.${table}", TableStorageConnectorConfig.DefaultTopicPattern);
        Assert.Equal("Timestamp", TableStorageConnectorConfig.DefaultIncrementalColumn);
    }

    [Fact]
    public void WriteModes_HaveExpectedValues()
    {
        Assert.Equal("upsert", TableStorageConnectorConfig.WriteModeUpsert);
        Assert.Equal("insert", TableStorageConnectorConfig.WriteModeInsert);
        Assert.Equal("update", TableStorageConnectorConfig.WriteModeUpdate);
        Assert.Equal("delete", TableStorageConnectorConfig.WriteModeDelete);
    }

    [Fact]
    public void IncrementalModes_HaveExpectedValues()
    {
        Assert.Equal("none", TableStorageConnectorConfig.IncrementalModeNone);
        Assert.Equal("timestamp", TableStorageConnectorConfig.IncrementalModeTimestamp);
        Assert.Equal("rowkey", TableStorageConnectorConfig.IncrementalModeRowKey);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("table.name", TableStorageConnectorConfig.HeaderTableName);
        Assert.Equal("table.partition.key", TableStorageConnectorConfig.HeaderPartitionKey);
        Assert.Equal("table.row.key", TableStorageConnectorConfig.HeaderRowKey);
        Assert.Equal("table.timestamp", TableStorageConnectorConfig.HeaderTimestamp);
        Assert.Equal("table.etag", TableStorageConnectorConfig.HeaderEtag);
    }
}
