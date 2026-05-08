namespace Kuestenlogik.Surgewave.Connector.Redis.List;

/// <summary>
/// Configuration constants for Redis List connector.
/// </summary>
public static class RedisListConnectorConfig
{
    public const string ConnectionString = "redis.connection.string";
    public const string Key = "redis.key";
    public const string Topic = "topic";

    // Source settings
    public const string PopDirection = "redis.pop.direction";
    public const string BlockingTimeoutMs = "redis.blocking.timeout.ms";
    public const string BatchSize = "redis.batch.size";

    // Sink settings
    public const string PushDirection = "redis.push.direction";

    // Defaults
    public const string DefaultConnectionString = "localhost:6379";
    public const string DefaultPopDirection = "left";
    public const string DefaultPushDirection = "right";
    public const int DefaultBlockingTimeoutMs = 5000;
    public const int DefaultBatchSize = 100;
}
