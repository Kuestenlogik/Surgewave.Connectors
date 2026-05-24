namespace Kuestenlogik.Surgewave.Connector.Redis;

/// <summary>
/// Shared configuration constants for Redis connectors.
/// </summary>
internal static class RedisConnectorConfig
{
    // Common configs
    public const string ConnectionConfig = "redis.connection";
    public const string ModeConfig = "redis.mode";
    public const string KeyPrefixConfig = "redis.key.prefix";

    // Sink-specific configs
    public const string TopicsConfig = "topics";
    public const string TtlSecondsConfig = "redis.ttl.seconds";
    public const string BatchSizeConfig = "batch.size";
    public const string HashKeyFieldConfig = "redis.hash.key.field";
    public const string StreamNameConfig = "redis.stream.name";
    public const string RetryMaxConfig = "retry.max";
    public const string RetryBackoffMsConfig = "retry.backoff.ms";

    // Source-specific configs
    public const string TopicConfig = "topic";
    public const string StreamsConfig = "redis.streams";
    public const string ConsumerGroupConfig = "redis.consumer.group";
    public const string ConsumerNameConfig = "redis.consumer.name";
    public const string PollIntervalMsConfig = "poll.interval.ms";
    public const string BatchMaxRecordsConfig = "batch.max.records";
    public const string PubSubChannelsConfig = "redis.pubsub.channels";

    // Modes
    public const string ModeString = "string";
    public const string ModeHash = "hash";
    public const string ModeStream = "stream";
    public const string ModePubSub = "pubsub";

    // Default values
    public const int DefaultBatchSize = 100;
    public const int DefaultPollIntervalMs = 1000;
    public const int DefaultBatchMaxRecords = 500;
    public const int DefaultRetryMax = 3;
    public const long DefaultRetryBackoffMs = 1000;
}
