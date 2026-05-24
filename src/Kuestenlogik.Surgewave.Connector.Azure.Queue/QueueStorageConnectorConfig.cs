namespace Kuestenlogik.Surgewave.Connector.Azure.Queue;

/// <summary>
/// Configuration constants for Azure Queue Storage connectors.
/// </summary>
public static class QueueStorageConnectorConfig
{
    // Connection configs
    public const string ConnectionStringConfig = "azure.queue.connection.string";
    public const string AccountNameConfig = "azure.queue.account.name";
    public const string AccountKeyConfig = "azure.queue.account.key";
    public const string EndpointConfig = "azure.queue.endpoint";

    // Queue configs
    public const string QueueNameConfig = "azure.queue.name";
    public const string TopicsConfig = "topics";

    // Source configs
    public const string PollIntervalMsConfig = "azure.queue.poll.interval.ms";
    public const string MaxMessagesPerPollConfig = "azure.queue.max.messages.per.poll";
    public const string VisibilityTimeoutSecondsConfig = "azure.queue.visibility.timeout.seconds";
    public const string TopicPatternConfig = "azure.queue.topic.pattern";
    public const string IncludeMetadataConfig = "azure.queue.include.metadata";
    public const string DeleteAfterReadConfig = "azure.queue.delete.after.read";
    public const string Base64DecodeConfig = "azure.queue.base64.decode";

    // Sink configs
    public const string TimeToLiveSecondsConfig = "azure.queue.time.to.live.seconds";
    public const string BatchSizeConfig = "azure.queue.batch.size";
    public const string Base64EncodeConfig = "azure.queue.base64.encode";
    public const string MaxRetryCountConfig = "azure.queue.max.retry.count";
    public const string RetryDelayMsConfig = "azure.queue.retry.delay.ms";
    public const string AutoCreateQueueConfig = "azure.queue.auto.create";

    // Offset tracking keys
    public const string OffsetMessageId = "message_id";
    public const string OffsetPopReceipt = "pop_receipt";
    public const string OffsetDequeueCount = "dequeue_count";

    // Default values
    public const long DefaultPollIntervalMs = 1000;
    public const int DefaultMaxMessagesPerPoll = 32;
    public const int DefaultVisibilityTimeoutSeconds = 30;
    public const int DefaultTimeToLiveSeconds = -1; // Never expires
    public const int DefaultBatchSize = 32;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;
    public const string DefaultTopicPattern = "queue.${queue}";

    // Header names
    public const string HeaderQueueName = "queue.name";
    public const string HeaderMessageId = "queue.message.id";
    public const string HeaderPopReceipt = "queue.pop.receipt";
    public const string HeaderDequeueCount = "queue.dequeue.count";
    public const string HeaderInsertedOn = "queue.inserted.on";
    public const string HeaderExpiresOn = "queue.expires.on";
    public const string HeaderNextVisibleOn = "queue.next.visible.on";
}
