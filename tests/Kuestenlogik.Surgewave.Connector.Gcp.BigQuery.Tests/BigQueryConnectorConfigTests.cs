using Kuestenlogik.Surgewave.Connector.Gcp.BigQuery;

namespace Kuestenlogik.Surgewave.Connector.Gcp.BigQuery.Tests;

public class BigQueryConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("gcp.bigquery.project.id", BigQueryConnectorConfig.ProjectIdConfig);
        Assert.Equal("gcp.bigquery.credentials.json", BigQueryConnectorConfig.CredentialsJsonConfig);
        Assert.Equal("gcp.bigquery.credentials.file", BigQueryConnectorConfig.CredentialsFileConfig);
        Assert.Equal("gcp.bigquery.dataset", BigQueryConnectorConfig.DatasetConfig);
        Assert.Equal("gcp.bigquery.location", BigQueryConnectorConfig.LocationConfig);
    }

    [Fact]
    public void SourceSettings_HaveExpectedValues()
    {
        Assert.Equal("gcp.bigquery.table", BigQueryConnectorConfig.TableConfig);
        Assert.Equal("gcp.bigquery.query", BigQueryConnectorConfig.QueryConfig);
        Assert.Equal("gcp.bigquery.topic.pattern", BigQueryConnectorConfig.TopicPatternConfig);
        Assert.Equal("gcp.bigquery.poll.interval.ms", BigQueryConnectorConfig.PollIntervalMsConfig);
        Assert.Equal("gcp.bigquery.max.rows.per.poll", BigQueryConnectorConfig.MaxRowsPerPollConfig);
        Assert.Equal("gcp.bigquery.include.metadata", BigQueryConnectorConfig.IncludeMetadataConfig);
        Assert.Equal("gcp.bigquery.mode", BigQueryConnectorConfig.ModeConfig);
        Assert.Equal("gcp.bigquery.timestamp.column", BigQueryConnectorConfig.TimestampColumnConfig);
        Assert.Equal("gcp.bigquery.partition.field", BigQueryConnectorConfig.PartitionFieldConfig);
        Assert.Equal("gcp.bigquery.use.standard.sql", BigQueryConnectorConfig.UseStandardSqlConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("topics", BigQueryConnectorConfig.TopicsConfig);
        Assert.Equal("gcp.bigquery.write.mode", BigQueryConnectorConfig.WriteModeConfig);
        Assert.Equal("gcp.bigquery.batch.size", BigQueryConnectorConfig.BatchSizeConfig);
        Assert.Equal("gcp.bigquery.max.retry.count", BigQueryConnectorConfig.MaxRetryCountConfig);
        Assert.Equal("gcp.bigquery.retry.delay.ms", BigQueryConnectorConfig.RetryDelayMsConfig);
        Assert.Equal("gcp.bigquery.auto.create.table", BigQueryConnectorConfig.AutoCreateTableConfig);
        Assert.Equal("gcp.bigquery.auto.create.dataset", BigQueryConnectorConfig.AutoCreateDatasetConfig);
        Assert.Equal("gcp.bigquery.use.streaming", BigQueryConnectorConfig.UseStreamingConfig);
        Assert.Equal("gcp.bigquery.schema.update.options", BigQueryConnectorConfig.SchemaUpdateOptionsConfig);
        Assert.Equal("gcp.bigquery.time.partitioning", BigQueryConnectorConfig.TimePartitioningConfig);
        Assert.Equal("gcp.bigquery.clustering.fields", BigQueryConnectorConfig.ClusteringFieldsConfig);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("bigquery.${project}.${dataset}.${table}", BigQueryConnectorConfig.DefaultTopicPattern);
        Assert.Equal(60000L, BigQueryConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(10000, BigQueryConnectorConfig.DefaultMaxRowsPerPoll);
        Assert.Equal("append", BigQueryConnectorConfig.DefaultWriteMode);
        Assert.Equal(10000, BigQueryConnectorConfig.DefaultBatchSize);
        Assert.Equal(3, BigQueryConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, BigQueryConnectorConfig.DefaultRetryDelayMs);
        Assert.Equal("table", BigQueryConnectorConfig.DefaultMode);
        Assert.Equal("US", BigQueryConnectorConfig.DefaultLocation);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("bigquery.project.id", BigQueryConnectorConfig.HeaderProjectId);
        Assert.Equal("bigquery.dataset", BigQueryConnectorConfig.HeaderDataset);
        Assert.Equal("bigquery.table", BigQueryConnectorConfig.HeaderTable);
        Assert.Equal("bigquery.location", BigQueryConnectorConfig.HeaderLocation);
        Assert.Equal("bigquery.partition.time", BigQueryConnectorConfig.HeaderPartitionTime);
        Assert.Equal("bigquery.insert.id", BigQueryConnectorConfig.HeaderInsertId);
        Assert.Equal("bigquery.timestamp", BigQueryConnectorConfig.HeaderTimestamp);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("timestamp", BigQueryConnectorConfig.OffsetTimestamp);
        Assert.Equal("partition", BigQueryConnectorConfig.OffsetPartition);
        Assert.Equal("row_number", BigQueryConnectorConfig.OffsetRowNumber);
        Assert.Equal("table", BigQueryConnectorConfig.OffsetTable);
    }

    [Fact]
    public void DefaultWriteMode_IsAppend()
    {
        Assert.Equal("append", BigQueryConnectorConfig.DefaultWriteMode);
    }

    [Fact]
    public void DefaultMode_IsTable()
    {
        Assert.Equal("table", BigQueryConnectorConfig.DefaultMode);
    }

    [Fact]
    public void DefaultLocation_IsUS()
    {
        Assert.Equal("US", BigQueryConnectorConfig.DefaultLocation);
    }

    [Fact]
    public void DefaultPollInterval_IsReasonable()
    {
        Assert.Equal(60000L, BigQueryConnectorConfig.DefaultPollIntervalMs);
        Assert.True(BigQueryConnectorConfig.DefaultPollIntervalMs >= 1000);
    }

    [Fact]
    public void DefaultBatchSize_IsReasonable()
    {
        Assert.Equal(10000, BigQueryConnectorConfig.DefaultBatchSize);
        Assert.True(BigQueryConnectorConfig.DefaultBatchSize > 0);
    }

    [Fact]
    public void DefaultMaxRowsPerPoll_IsReasonable()
    {
        Assert.Equal(10000, BigQueryConnectorConfig.DefaultMaxRowsPerPoll);
        Assert.True(BigQueryConnectorConfig.DefaultMaxRowsPerPoll > 0);
    }

    [Fact]
    public void DefaultRetrySettings_AreReasonable()
    {
        Assert.Equal(3, BigQueryConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, BigQueryConnectorConfig.DefaultRetryDelayMs);
        Assert.True(BigQueryConnectorConfig.DefaultMaxRetryCount > 0);
        Assert.True(BigQueryConnectorConfig.DefaultRetryDelayMs > 0);
    }
}
