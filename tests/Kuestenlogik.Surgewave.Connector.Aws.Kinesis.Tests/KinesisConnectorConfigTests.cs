using Xunit;
using Kuestenlogik.Surgewave.Connector.Aws.Kinesis;

namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis.Tests;

public class KinesisConnectorConfigTests
{
    [Fact]
    public void AwsConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("aws.region", KinesisConnectorConfig.RegionConfig);
        Assert.Equal("aws.access.key", KinesisConnectorConfig.AccessKeyConfig);
        Assert.Equal("aws.secret.key", KinesisConnectorConfig.SecretKeyConfig);
        Assert.Equal("aws.endpoint", KinesisConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void StreamSettings_HaveExpectedValues()
    {
        Assert.Equal("aws.kinesis.stream.name", KinesisConnectorConfig.StreamNameConfig);
        Assert.Equal("aws.kinesis.stream.arn", KinesisConnectorConfig.StreamArnConfig);
        Assert.Equal("topics", KinesisConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void SourceSettings_HaveExpectedValues()
    {
        Assert.Equal("aws.kinesis.shard.iterator.type", KinesisConnectorConfig.ShardIteratorTypeConfig);
        Assert.Equal("aws.kinesis.poll.interval.ms", KinesisConnectorConfig.PollIntervalMsConfig);
        Assert.Equal("aws.kinesis.batch.max.records", KinesisConnectorConfig.BatchMaxRecordsConfig);
        Assert.Equal("aws.kinesis.start.from.beginning", KinesisConnectorConfig.StartFromBeginningConfig);
        Assert.Equal("aws.kinesis.start.timestamp", KinesisConnectorConfig.StartTimestampConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("aws.kinesis.partition.key.field", KinesisConnectorConfig.PartitionKeyFieldConfig);
        Assert.Equal("aws.kinesis.explicit.hash.key.field", KinesisConnectorConfig.ExplicitHashKeyFieldConfig);
        Assert.Equal("aws.kinesis.batch.size", KinesisConnectorConfig.BatchSizeConfig);
        Assert.Equal("aws.kinesis.batch.bytes", KinesisConnectorConfig.BatchBytesConfig);
        Assert.Equal("aws.kinesis.retry.count", KinesisConnectorConfig.RetryCountConfig);
        Assert.Equal("aws.kinesis.retry.delay.ms", KinesisConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void TopicPatternSettings_HaveExpectedValues()
    {
        Assert.Equal("aws.kinesis.topic.pattern", KinesisConnectorConfig.TopicPatternConfig);
        Assert.Equal("aws.kinesis.include.metadata", KinesisConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("shard_id", KinesisConnectorConfig.OffsetShardId);
        Assert.Equal("sequence_number", KinesisConnectorConfig.OffsetSequenceNumber);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("us-east-1", KinesisConnectorConfig.DefaultRegion);
        Assert.Equal(500L, KinesisConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(100, KinesisConnectorConfig.DefaultBatchMaxRecords);
        Assert.Equal(500, KinesisConnectorConfig.DefaultBatchSize);
        Assert.Equal(5 * 1024 * 1024, KinesisConnectorConfig.DefaultBatchBytes);
        Assert.Equal(3, KinesisConnectorConfig.DefaultRetryCount);
        Assert.Equal(100L, KinesisConnectorConfig.DefaultRetryDelayMs);
        Assert.Equal("kinesis.${stream}", KinesisConnectorConfig.DefaultTopicPattern);
    }

    [Fact]
    public void ShardIteratorTypes_HaveExpectedValues()
    {
        Assert.Equal("TRIM_HORIZON", KinesisConnectorConfig.ShardIteratorTrimHorizon);
        Assert.Equal("LATEST", KinesisConnectorConfig.ShardIteratorLatest);
        Assert.Equal("AT_SEQUENCE_NUMBER", KinesisConnectorConfig.ShardIteratorAtSequenceNumber);
        Assert.Equal("AFTER_SEQUENCE_NUMBER", KinesisConnectorConfig.ShardIteratorAfterSequenceNumber);
        Assert.Equal("AT_TIMESTAMP", KinesisConnectorConfig.ShardIteratorAtTimestamp);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("kinesis.stream", KinesisConnectorConfig.HeaderStreamName);
        Assert.Equal("kinesis.partition.key", KinesisConnectorConfig.HeaderPartitionKey);
        Assert.Equal("kinesis.sequence", KinesisConnectorConfig.HeaderSequenceNumber);
        Assert.Equal("kinesis.shard", KinesisConnectorConfig.HeaderShardId);
        Assert.Equal("kinesis.arrival.timestamp", KinesisConnectorConfig.HeaderApproximateArrivalTimestamp);
    }
}
