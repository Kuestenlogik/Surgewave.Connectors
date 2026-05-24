namespace Kuestenlogik.Surgewave.Connector.Redis.Scan;

/// <summary>
/// Configuration constants for Redis Scan connector.
/// </summary>
public static class RedisScanConnectorConfig
{
    public const string ConnectionString = "redis.connection.string";
    public const string Pattern = "redis.pattern";
    public const string Topic = "topic";
    public const string KeyType = "redis.key.type";
    public const string IncludeValue = "redis.include.value";
    public const string BatchSize = "redis.batch.size";
    public const string PollIntervalMs = "redis.poll.interval.ms";
    public const string Database = "redis.database";

    // Defaults
    public const string DefaultConnectionString = "localhost:6379";
    public const string DefaultPattern = "*";
    public const string DefaultKeyType = "";
    public const bool DefaultIncludeValue = true;
    public const int DefaultBatchSize = 1000;
    public const int DefaultPollIntervalMs = 60000;
    public const int DefaultDatabase = 0;
}
