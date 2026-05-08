namespace Kuestenlogik.Surgewave.Connector.Aws.Sqs;

/// <summary>
/// Configuration constants for AWS SQS connectors.
/// </summary>
internal static class SqsConnectorConfig
{
    // AWS connection configs
    public const string RegionConfig = "aws.region";
    public const string AccessKeyConfig = "aws.access.key";
    public const string SecretKeyConfig = "aws.secret.key";
    public const string EndpointConfig = "aws.endpoint";

    // SQS configs
    public const string QueueUrlConfig = "aws.sqs.queue.url";
    public const string SurgewaveTopicConfig = "surgewave.topic";
    public const string TopicsConfig = "topics";
    public const string WaitTimeSecondsConfig = "aws.sqs.wait.time.seconds";
    public const string VisibilityTimeoutConfig = "aws.sqs.visibility.timeout";
    public const string MaxMessagesConfig = "aws.sqs.max.messages";
    public const string MessageGroupIdFieldConfig = "aws.sqs.message.group.id.field";
    public const string DeduplicationIdFieldConfig = "aws.sqs.deduplication.id.field";

    // Header mapping
    public const string HeaderPrefixConfig = "aws.sqs.header.prefix";
    public const string IncludeMetadataConfig = "aws.sqs.include.metadata";

    // Default values
    public const int DefaultWaitTimeSeconds = 20; // Long polling
    public const int DefaultVisibilityTimeout = 30;
    public const int DefaultMaxMessages = 10; // SQS max is 10
    public const string DefaultRegion = "us-east-1";
    public const string DefaultHeaderPrefix = "sqs.";
    public const bool DefaultIncludeMetadata = true;
}
