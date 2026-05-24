using Kuestenlogik.Surgewave.Connector.Cassandra;

namespace Kuestenlogik.Surgewave.Connector.Cassandra.Tests;

public class CassandraConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("cassandra.contact.points", CassandraConnectorConfig.ContactPointsConfig);
        Assert.Equal("cassandra.port", CassandraConnectorConfig.PortConfig);
        Assert.Equal("cassandra.datacenter", CassandraConnectorConfig.DatacenterConfig);
        Assert.Equal("cassandra.keyspace", CassandraConnectorConfig.KeyspaceConfig);
        Assert.Equal("cassandra.username", CassandraConnectorConfig.UsernameConfig);
        Assert.Equal("cassandra.password", CassandraConnectorConfig.PasswordConfig);
        Assert.Equal("cassandra.consistency", CassandraConnectorConfig.ConsistencyLevelConfig);
        Assert.Equal("cassandra.ssl.enabled", CassandraConnectorConfig.SslEnabledConfig);
    }

    [Fact]
    public void SourceSettings_HaveExpectedValues()
    {
        Assert.Equal("cassandra.table", CassandraConnectorConfig.TableConfig);
        Assert.Equal("cassandra.topic.pattern", CassandraConnectorConfig.TopicPatternConfig);
        Assert.Equal("cassandra.poll.interval.ms", CassandraConnectorConfig.PollIntervalMsConfig);
        Assert.Equal("cassandra.max.rows.per.poll", CassandraConnectorConfig.MaxRowsPerPollConfig);
        Assert.Equal("cassandra.include.metadata", CassandraConnectorConfig.IncludeMetadataConfig);
        Assert.Equal("cassandra.mode", CassandraConnectorConfig.ModeConfig);
        Assert.Equal("cassandra.query", CassandraConnectorConfig.QueryConfig);
        Assert.Equal("cassandra.timestamp.column", CassandraConnectorConfig.TimestampColumnConfig);
        Assert.Equal("cassandra.partition.key.columns", CassandraConnectorConfig.PartitionKeyColumnsConfig);
        Assert.Equal("cassandra.clustering.key.columns", CassandraConnectorConfig.ClusteringKeyColumnsConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("topics", CassandraConnectorConfig.TopicsConfig);
        Assert.Equal("cassandra.write.mode", CassandraConnectorConfig.WriteModeConfig);
        Assert.Equal("cassandra.batch.size", CassandraConnectorConfig.BatchSizeConfig);
        Assert.Equal("cassandra.max.retry.count", CassandraConnectorConfig.MaxRetryCountConfig);
        Assert.Equal("cassandra.retry.delay.ms", CassandraConnectorConfig.RetryDelayMsConfig);
        Assert.Equal("cassandra.batch.type", CassandraConnectorConfig.BatchTypeConfig);
        Assert.Equal("cassandra.ttl.seconds", CassandraConnectorConfig.TtlSecondsConfig);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("cassandra.${keyspace}.${table}", CassandraConnectorConfig.DefaultTopicPattern);
        Assert.Equal(9042, CassandraConnectorConfig.DefaultPort);
        Assert.Equal(5000L, CassandraConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(10000, CassandraConnectorConfig.DefaultMaxRowsPerPoll);
        Assert.Equal("insert", CassandraConnectorConfig.DefaultWriteMode);
        Assert.Equal("table", CassandraConnectorConfig.DefaultMode);
        Assert.Equal("LOCAL_QUORUM", CassandraConnectorConfig.DefaultConsistencyLevel);
        Assert.Equal(500, CassandraConnectorConfig.DefaultBatchSize);
        Assert.Equal(3, CassandraConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, CassandraConnectorConfig.DefaultRetryDelayMs);
        Assert.Equal("unlogged", CassandraConnectorConfig.DefaultBatchType);
        Assert.Equal(0, CassandraConnectorConfig.DefaultTtlSeconds);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("cassandra.keyspace", CassandraConnectorConfig.HeaderKeyspace);
        Assert.Equal("cassandra.table", CassandraConnectorConfig.HeaderTable);
        Assert.Equal("cassandra.partition.key", CassandraConnectorConfig.HeaderPartitionKey);
        Assert.Equal("cassandra.clustering.key", CassandraConnectorConfig.HeaderClusteringKey);
        Assert.Equal("cassandra.writetime", CassandraConnectorConfig.HeaderWriteTime);
        Assert.Equal("cassandra.ttl", CassandraConnectorConfig.HeaderTtl);
        Assert.Equal("cassandra.timestamp", CassandraConnectorConfig.HeaderTimestamp);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("timestamp", CassandraConnectorConfig.OffsetTimestamp);
        Assert.Equal("partition_key", CassandraConnectorConfig.OffsetPartitionKey);
        Assert.Equal("clustering_key", CassandraConnectorConfig.OffsetClusteringKey);
        Assert.Equal("table", CassandraConnectorConfig.OffsetTable);
    }

    [Fact]
    public void DefaultWriteMode_IsInsert()
    {
        Assert.Equal("insert", CassandraConnectorConfig.DefaultWriteMode);
    }

    [Fact]
    public void DefaultMode_IsTable()
    {
        Assert.Equal("table", CassandraConnectorConfig.DefaultMode);
    }

    [Fact]
    public void DefaultConsistencyLevel_IsLocalQuorum()
    {
        Assert.Equal("LOCAL_QUORUM", CassandraConnectorConfig.DefaultConsistencyLevel);
    }

    [Fact]
    public void DefaultPort_Is9042()
    {
        Assert.Equal(9042, CassandraConnectorConfig.DefaultPort);
    }

    [Fact]
    public void DefaultPollInterval_IsReasonable()
    {
        Assert.Equal(5000L, CassandraConnectorConfig.DefaultPollIntervalMs);
        Assert.True(CassandraConnectorConfig.DefaultPollIntervalMs >= 1000);
    }

    [Fact]
    public void DefaultBatchSize_IsReasonable()
    {
        Assert.Equal(500, CassandraConnectorConfig.DefaultBatchSize);
        Assert.True(CassandraConnectorConfig.DefaultBatchSize > 0);
    }

    [Fact]
    public void DefaultMaxRowsPerPoll_IsReasonable()
    {
        Assert.Equal(10000, CassandraConnectorConfig.DefaultMaxRowsPerPoll);
        Assert.True(CassandraConnectorConfig.DefaultMaxRowsPerPoll > 0);
    }

    [Fact]
    public void DefaultRetrySettings_AreReasonable()
    {
        Assert.Equal(3, CassandraConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, CassandraConnectorConfig.DefaultRetryDelayMs);
        Assert.True(CassandraConnectorConfig.DefaultMaxRetryCount > 0);
        Assert.True(CassandraConnectorConfig.DefaultRetryDelayMs > 0);
    }
}
