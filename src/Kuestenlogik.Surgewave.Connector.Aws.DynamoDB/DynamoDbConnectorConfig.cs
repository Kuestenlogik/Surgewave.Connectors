namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;

/// <summary>
/// Configuration constants for AWS DynamoDB connectors.
/// </summary>
public static class DynamoDbConnectorConfig
{
    // AWS connection configs
    public const string RegionConfig = "aws.region";
    public const string AccessKeyConfig = "aws.access.key";
    public const string SecretKeyConfig = "aws.secret.key";
    public const string EndpointConfig = "aws.endpoint";

    // DynamoDB table configs
    public const string TableNameConfig = "aws.dynamodb.table.name";
    public const string TopicsConfig = "topics";

    // Source (Streams) configs
    public const string StreamArnConfig = "aws.dynamodb.stream.arn";
    public const string StreamViewTypeConfig = "aws.dynamodb.stream.view.type";
    public const string ShardIteratorTypeConfig = "aws.dynamodb.shard.iterator.type";
    public const string PollIntervalMsConfig = "aws.dynamodb.poll.interval.ms";
    public const string BatchMaxRecordsConfig = "aws.dynamodb.batch.max.records";
    public const string StartFromBeginningConfig = "aws.dynamodb.start.from.beginning";

    // Sink configs
    public const string WriteModeConfig = "aws.dynamodb.write.mode";
    public const string BatchSizeConfig = "aws.dynamodb.batch.size";
    public const string KeyFieldsConfig = "aws.dynamodb.key.fields";
    public const string PartitionKeyFieldConfig = "aws.dynamodb.partition.key.field";
    public const string SortKeyFieldConfig = "aws.dynamodb.sort.key.field";
    public const string AutoCreateTableConfig = "aws.dynamodb.auto.create.table";
    public const string ReadCapacityConfig = "aws.dynamodb.read.capacity";
    public const string WriteCapacityConfig = "aws.dynamodb.write.capacity";
    public const string BillingModeConfig = "aws.dynamodb.billing.mode";

    // Topic pattern
    public const string TopicPatternConfig = "aws.dynamodb.topic.pattern";
    public const string IncludeMetadataConfig = "aws.dynamodb.include.metadata";

    // Offset tracking keys
    public const string OffsetShardId = "shard_id";
    public const string OffsetSequenceNumber = "sequence_number";

    // Default values
    public const string DefaultRegion = "us-east-1";
    public const long DefaultPollIntervalMs = 500;
    public const int DefaultBatchMaxRecords = 100;
    public const int DefaultBatchSize = 25; // DynamoDB batch limit
    public const long DefaultReadCapacity = 5;
    public const long DefaultWriteCapacity = 5;
    public const string DefaultTopicPattern = "dynamodb.${table}";

    // Write modes
    public const string WriteModeInsert = "insert";
    public const string WriteModePut = "put";
    public const string WriteModeUpdate = "update";
    public const string WriteModeDelete = "delete";

    // Shard iterator types
    public const string ShardIteratorTrimHorizon = "TRIM_HORIZON";
    public const string ShardIteratorLatest = "LATEST";
    public const string ShardIteratorAtSequenceNumber = "AT_SEQUENCE_NUMBER";
    public const string ShardIteratorAfterSequenceNumber = "AFTER_SEQUENCE_NUMBER";

    // Stream view types
    public const string StreamViewKeysOnly = "KEYS_ONLY";
    public const string StreamViewNewImage = "NEW_IMAGE";
    public const string StreamViewOldImage = "OLD_IMAGE";
    public const string StreamViewNewAndOldImages = "NEW_AND_OLD_IMAGES";

    // Billing modes
    public const string BillingModeProvisioned = "PROVISIONED";
    public const string BillingModePayPerRequest = "PAY_PER_REQUEST";

    // DynamoDB event names (from streams)
    public const string EventInsert = "INSERT";
    public const string EventModify = "MODIFY";
    public const string EventRemove = "REMOVE";

    // Header names
    public const string HeaderTableName = "dynamodb.table";
    public const string HeaderEventName = "dynamodb.event";
    public const string HeaderSequenceNumber = "dynamodb.sequence";
    public const string HeaderShardId = "dynamodb.shard";
}
