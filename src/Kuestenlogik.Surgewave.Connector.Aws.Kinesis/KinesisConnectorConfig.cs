namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis;

/// <summary>
/// Configuration constants for AWS Kinesis connectors.
/// </summary>
public static class KinesisConnectorConfig
{
    // AWS connection configs
    public const string RegionConfig = "aws.region";
    public const string AccessKeyConfig = "aws.access.key";
    public const string SecretKeyConfig = "aws.secret.key";
    public const string EndpointConfig = "aws.endpoint";

    // Kinesis stream configs
    public const string StreamNameConfig = "aws.kinesis.stream.name";
    public const string StreamArnConfig = "aws.kinesis.stream.arn";
    public const string TopicsConfig = "topics";

    // Source configs
    public const string ShardIteratorTypeConfig = "aws.kinesis.shard.iterator.type";
    public const string PollIntervalMsConfig = "aws.kinesis.poll.interval.ms";
    public const string BatchMaxRecordsConfig = "aws.kinesis.batch.max.records";
    public const string StartFromBeginningConfig = "aws.kinesis.start.from.beginning";
    public const string StartTimestampConfig = "aws.kinesis.start.timestamp";

    // Sink configs
    public const string PartitionKeyFieldConfig = "aws.kinesis.partition.key.field";
    public const string ExplicitHashKeyFieldConfig = "aws.kinesis.explicit.hash.key.field";
    public const string BatchSizeConfig = "aws.kinesis.batch.size";
    public const string BatchBytesConfig = "aws.kinesis.batch.bytes";
    public const string RetryCountConfig = "aws.kinesis.retry.count";
    public const string RetryDelayMsConfig = "aws.kinesis.retry.delay.ms";

    // Topic pattern
    public const string TopicPatternConfig = "aws.kinesis.topic.pattern";
    public const string IncludeMetadataConfig = "aws.kinesis.include.metadata";

    // Offset tracking keys
    public const string OffsetShardId = "shard_id";
    public const string OffsetSequenceNumber = "sequence_number";

    // Default values
    public const string DefaultRegion = "us-east-1";
    public const long DefaultPollIntervalMs = 500;
    public const int DefaultBatchMaxRecords = 100;
    public const int DefaultBatchSize = 500; // Kinesis PutRecords limit
    public const int DefaultBatchBytes = 5 * 1024 * 1024; // 5MB limit
    public const int DefaultRetryCount = 3;
    public const long DefaultRetryDelayMs = 100;
    public const string DefaultTopicPattern = "kinesis.${stream}";

    // Shard iterator types
    public const string ShardIteratorTrimHorizon = "TRIM_HORIZON";
    public const string ShardIteratorLatest = "LATEST";
    public const string ShardIteratorAtSequenceNumber = "AT_SEQUENCE_NUMBER";
    public const string ShardIteratorAfterSequenceNumber = "AFTER_SEQUENCE_NUMBER";
    public const string ShardIteratorAtTimestamp = "AT_TIMESTAMP";

    // Header names
    public const string HeaderStreamName = "kinesis.stream";
    public const string HeaderPartitionKey = "kinesis.partition.key";
    public const string HeaderSequenceNumber = "kinesis.sequence";
    public const string HeaderShardId = "kinesis.shard";
    public const string HeaderApproximateArrivalTimestamp = "kinesis.arrival.timestamp";
}
