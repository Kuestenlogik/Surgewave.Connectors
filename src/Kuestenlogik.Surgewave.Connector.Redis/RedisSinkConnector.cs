namespace Kuestenlogik.Surgewave.Connector.Redis;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that writes records to Redis.
/// Supports string (SET), hash (HSET), and stream (XADD) modes.
/// </summary>
public sealed class RedisSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(RedisSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(RedisConnectorConfig.ConnectionConfig, ConfigType.Password, Importance.High,
            "Redis connection string (e.g., 'localhost:6379,password=secret')")
        .Define(RedisConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(RedisConnectorConfig.ModeConfig, ConfigType.String, RedisConnectorConfig.ModeString, Importance.High,
            "Redis mode: 'string' (SET), 'hash' (HSET), or 'stream' (XADD)", EditorHint.Select, options: ["string", "hash", "stream", "pubsub"])
        .Define(RedisConnectorConfig.KeyPrefixConfig, ConfigType.String, "", Importance.Medium,
            "Optional prefix for all Redis keys")
        .Define(RedisConnectorConfig.TtlSecondsConfig, ConfigType.Int, 0L, Importance.Medium,
            "TTL in seconds for string/hash keys (0 = no expiry)")
        .Define(RedisConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)RedisConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to buffer before flushing")
        .Define(RedisConnectorConfig.HashKeyFieldConfig, ConfigType.String, "", Importance.Medium,
            "Field in record JSON to use as hash key (hash mode only)")
        .Define(RedisConnectorConfig.StreamNameConfig, ConfigType.String, "${topic}", Importance.Medium,
            "Redis stream name pattern (supports ${topic} substitution)")
        .Define(RedisConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)RedisConnectorConfig.DefaultRetryMax, Importance.Medium,
            "Maximum retry attempts on failure")
        .Define(RedisConnectorConfig.RetryBackoffMsConfig, ConfigType.Long, RedisConnectorConfig.DefaultRetryBackoffMs, Importance.Medium,
            "Backoff time between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(RedisConnectorConfig.ConnectionConfig, out _))
            throw new ArgumentException($"Missing required config: {RedisConnectorConfig.ConnectionConfig}");

        if (!config.TryGetValue(RedisConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {RedisConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(RedisConnectorConfig.ModeConfig, out var modeValue) ? modeValue : RedisConnectorConfig.ModeString;
        if (mode is not (RedisConnectorConfig.ModeString or RedisConnectorConfig.ModeHash or RedisConnectorConfig.ModeStream))
            throw new ArgumentException($"Invalid mode '{mode}'. Must be '{RedisConnectorConfig.ModeString}', '{RedisConnectorConfig.ModeHash}', or '{RedisConnectorConfig.ModeStream}'");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for simplicity; could partition by topic for parallelism
        return [new Dictionary<string, string>(_config)];
    }
}
