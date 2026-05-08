using Kuestenlogik.Surgewave.Connector.InfluxDB;

namespace Kuestenlogik.Surgewave.Connector.InfluxDB.Tests;

public class InfluxDBConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("influxdb.url", InfluxDBConnectorConfig.UrlConfig);
        Assert.Equal("influxdb.token", InfluxDBConnectorConfig.TokenConfig);
        Assert.Equal("influxdb.org", InfluxDBConnectorConfig.OrgConfig);
        Assert.Equal("influxdb.bucket", InfluxDBConnectorConfig.BucketConfig);
    }

    [Fact]
    public void SourceSettings_HaveExpectedValues()
    {
        Assert.Equal("influxdb.topic.pattern", InfluxDBConnectorConfig.TopicPatternConfig);
        Assert.Equal("influxdb.poll.interval.ms", InfluxDBConnectorConfig.PollIntervalMsConfig);
        Assert.Equal("influxdb.max.rows.per.poll", InfluxDBConnectorConfig.MaxRowsPerPollConfig);
        Assert.Equal("influxdb.include.metadata", InfluxDBConnectorConfig.IncludeMetadataConfig);
        Assert.Equal("influxdb.query", InfluxDBConnectorConfig.QueryConfig);
        Assert.Equal("influxdb.measurement", InfluxDBConnectorConfig.MeasurementConfig);
        Assert.Equal("influxdb.start.time", InfluxDBConnectorConfig.StartTimeConfig);
        Assert.Equal("influxdb.stop.time", InfluxDBConnectorConfig.StopTimeConfig);
        Assert.Equal("influxdb.time.range", InfluxDBConnectorConfig.TimeRangeConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("topics", InfluxDBConnectorConfig.TopicsConfig);
        Assert.Equal("influxdb.batch.size", InfluxDBConnectorConfig.BatchSizeConfig);
        Assert.Equal("influxdb.max.retry.count", InfluxDBConnectorConfig.MaxRetryCountConfig);
        Assert.Equal("influxdb.retry.delay.ms", InfluxDBConnectorConfig.RetryDelayMsConfig);
        Assert.Equal("influxdb.measurement.field", InfluxDBConnectorConfig.MeasurementFieldConfig);
        Assert.Equal("influxdb.timestamp.field", InfluxDBConnectorConfig.TimestampFieldConfig);
        Assert.Equal("influxdb.tag.fields", InfluxDBConnectorConfig.TagFieldsConfig);
        Assert.Equal("influxdb.field.fields", InfluxDBConnectorConfig.FieldFieldsConfig);
        Assert.Equal("influxdb.precision", InfluxDBConnectorConfig.PrecisionConfig);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("influxdb.${org}.${bucket}.${measurement}", InfluxDBConnectorConfig.DefaultTopicPattern);
        Assert.Equal(10000L, InfluxDBConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(10000, InfluxDBConnectorConfig.DefaultMaxRowsPerPoll);
        Assert.Equal(5000, InfluxDBConnectorConfig.DefaultBatchSize);
        Assert.Equal(3, InfluxDBConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, InfluxDBConnectorConfig.DefaultRetryDelayMs);
        Assert.Equal("-1h", InfluxDBConnectorConfig.DefaultTimeRange);
        Assert.Equal("ns", InfluxDBConnectorConfig.DefaultPrecision);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("influxdb.org", InfluxDBConnectorConfig.HeaderOrg);
        Assert.Equal("influxdb.bucket", InfluxDBConnectorConfig.HeaderBucket);
        Assert.Equal("influxdb.measurement", InfluxDBConnectorConfig.HeaderMeasurement);
        Assert.Equal("influxdb.timestamp", InfluxDBConnectorConfig.HeaderTimestamp);
        Assert.Equal("influxdb.tags", InfluxDBConnectorConfig.HeaderTags);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("timestamp", InfluxDBConnectorConfig.OffsetTimestamp);
        Assert.Equal("measurement", InfluxDBConnectorConfig.OffsetMeasurement);
        Assert.Equal("bucket", InfluxDBConnectorConfig.OffsetBucket);
    }

    [Fact]
    public void DefaultPollInterval_IsReasonable()
    {
        Assert.Equal(10000L, InfluxDBConnectorConfig.DefaultPollIntervalMs);
        Assert.True(InfluxDBConnectorConfig.DefaultPollIntervalMs >= 1000);
    }

    [Fact]
    public void DefaultBatchSize_IsReasonable()
    {
        Assert.Equal(5000, InfluxDBConnectorConfig.DefaultBatchSize);
        Assert.True(InfluxDBConnectorConfig.DefaultBatchSize > 0);
    }

    [Fact]
    public void DefaultRetrySettings_AreReasonable()
    {
        Assert.Equal(3, InfluxDBConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, InfluxDBConnectorConfig.DefaultRetryDelayMs);
        Assert.True(InfluxDBConnectorConfig.DefaultMaxRetryCount > 0);
        Assert.True(InfluxDBConnectorConfig.DefaultRetryDelayMs > 0);
    }
}
