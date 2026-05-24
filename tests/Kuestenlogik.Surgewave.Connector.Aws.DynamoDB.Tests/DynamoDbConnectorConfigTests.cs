using Xunit;
using Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;

namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB.Tests;

public class DynamoDbConnectorConfigTests
{
    [Fact]
    public void AwsConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("aws.region", DynamoDbConnectorConfig.RegionConfig);
        Assert.Equal("aws.access.key", DynamoDbConnectorConfig.AccessKeyConfig);
        Assert.Equal("aws.secret.key", DynamoDbConnectorConfig.SecretKeyConfig);
        Assert.Equal("aws.endpoint", DynamoDbConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void TableSettings_HaveExpectedValues()
    {
        Assert.Equal("aws.dynamodb.table.name", DynamoDbConnectorConfig.TableNameConfig);
        Assert.Equal("topics", DynamoDbConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void StreamSettings_HaveExpectedValues()
    {
        Assert.Equal("aws.dynamodb.stream.arn", DynamoDbConnectorConfig.StreamArnConfig);
        Assert.Equal("aws.dynamodb.stream.view.type", DynamoDbConnectorConfig.StreamViewTypeConfig);
        Assert.Equal("aws.dynamodb.shard.iterator.type", DynamoDbConnectorConfig.ShardIteratorTypeConfig);
        Assert.Equal("aws.dynamodb.poll.interval.ms", DynamoDbConnectorConfig.PollIntervalMsConfig);
        Assert.Equal("aws.dynamodb.batch.max.records", DynamoDbConnectorConfig.BatchMaxRecordsConfig);
        Assert.Equal("aws.dynamodb.start.from.beginning", DynamoDbConnectorConfig.StartFromBeginningConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("aws.dynamodb.write.mode", DynamoDbConnectorConfig.WriteModeConfig);
        Assert.Equal("aws.dynamodb.batch.size", DynamoDbConnectorConfig.BatchSizeConfig);
        Assert.Equal("aws.dynamodb.key.fields", DynamoDbConnectorConfig.KeyFieldsConfig);
        Assert.Equal("aws.dynamodb.partition.key.field", DynamoDbConnectorConfig.PartitionKeyFieldConfig);
        Assert.Equal("aws.dynamodb.sort.key.field", DynamoDbConnectorConfig.SortKeyFieldConfig);
        Assert.Equal("aws.dynamodb.auto.create.table", DynamoDbConnectorConfig.AutoCreateTableConfig);
        Assert.Equal("aws.dynamodb.read.capacity", DynamoDbConnectorConfig.ReadCapacityConfig);
        Assert.Equal("aws.dynamodb.write.capacity", DynamoDbConnectorConfig.WriteCapacityConfig);
        Assert.Equal("aws.dynamodb.billing.mode", DynamoDbConnectorConfig.BillingModeConfig);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("shard_id", DynamoDbConnectorConfig.OffsetShardId);
        Assert.Equal("sequence_number", DynamoDbConnectorConfig.OffsetSequenceNumber);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("us-east-1", DynamoDbConnectorConfig.DefaultRegion);
        Assert.Equal(500L, DynamoDbConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(100, DynamoDbConnectorConfig.DefaultBatchMaxRecords);
        Assert.Equal(25, DynamoDbConnectorConfig.DefaultBatchSize);
        Assert.Equal(5L, DynamoDbConnectorConfig.DefaultReadCapacity);
        Assert.Equal(5L, DynamoDbConnectorConfig.DefaultWriteCapacity);
        Assert.Equal("dynamodb.${table}", DynamoDbConnectorConfig.DefaultTopicPattern);
    }

    [Fact]
    public void WriteModes_HaveExpectedValues()
    {
        Assert.Equal("insert", DynamoDbConnectorConfig.WriteModeInsert);
        Assert.Equal("put", DynamoDbConnectorConfig.WriteModePut);
        Assert.Equal("update", DynamoDbConnectorConfig.WriteModeUpdate);
        Assert.Equal("delete", DynamoDbConnectorConfig.WriteModeDelete);
    }

    [Fact]
    public void ShardIteratorTypes_HaveExpectedValues()
    {
        Assert.Equal("TRIM_HORIZON", DynamoDbConnectorConfig.ShardIteratorTrimHorizon);
        Assert.Equal("LATEST", DynamoDbConnectorConfig.ShardIteratorLatest);
        Assert.Equal("AT_SEQUENCE_NUMBER", DynamoDbConnectorConfig.ShardIteratorAtSequenceNumber);
        Assert.Equal("AFTER_SEQUENCE_NUMBER", DynamoDbConnectorConfig.ShardIteratorAfterSequenceNumber);
    }

    [Fact]
    public void StreamViewTypes_HaveExpectedValues()
    {
        Assert.Equal("KEYS_ONLY", DynamoDbConnectorConfig.StreamViewKeysOnly);
        Assert.Equal("NEW_IMAGE", DynamoDbConnectorConfig.StreamViewNewImage);
        Assert.Equal("OLD_IMAGE", DynamoDbConnectorConfig.StreamViewOldImage);
        Assert.Equal("NEW_AND_OLD_IMAGES", DynamoDbConnectorConfig.StreamViewNewAndOldImages);
    }

    [Fact]
    public void BillingModes_HaveExpectedValues()
    {
        Assert.Equal("PROVISIONED", DynamoDbConnectorConfig.BillingModeProvisioned);
        Assert.Equal("PAY_PER_REQUEST", DynamoDbConnectorConfig.BillingModePayPerRequest);
    }

    [Fact]
    public void EventNames_HaveExpectedValues()
    {
        Assert.Equal("INSERT", DynamoDbConnectorConfig.EventInsert);
        Assert.Equal("MODIFY", DynamoDbConnectorConfig.EventModify);
        Assert.Equal("REMOVE", DynamoDbConnectorConfig.EventRemove);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("dynamodb.table", DynamoDbConnectorConfig.HeaderTableName);
        Assert.Equal("dynamodb.event", DynamoDbConnectorConfig.HeaderEventName);
        Assert.Equal("dynamodb.sequence", DynamoDbConnectorConfig.HeaderSequenceNumber);
        Assert.Equal("dynamodb.shard", DynamoDbConnectorConfig.HeaderShardId);
    }
}
