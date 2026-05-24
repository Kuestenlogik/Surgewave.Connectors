using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Redis.List;

/// <summary>
/// Source connector for Redis list-based queues using BRPOP/BLPOP.
/// </summary>
[ConnectorMetadata(
    Name = "redis-list-source",
    Description = "Consumes items from Redis lists using blocking pop operations",
    Author = "Surgewave",
    Tags = "redis,list,queue,source")]
public sealed class RedisListSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(RedisListSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(RedisListConnectorConfig.ConnectionString, ConfigType.String, RedisListConnectorConfig.DefaultConnectionString, Importance.High, "Redis connection string")
        .Define(RedisListConnectorConfig.Key, ConfigType.String, Importance.High, "Redis list key to consume from")
        .Define(RedisListConnectorConfig.Topic, ConfigType.String, Importance.High, "Surgewave topic to write to", EditorHint.Topic)
        .Define(RedisListConnectorConfig.PopDirection, ConfigType.String, RedisListConnectorConfig.DefaultPopDirection, Importance.Medium, "Pop direction: left (BLPOP) or right (BRPOP)", EditorHint.Select, options: ["left", "right"])
        .Define(RedisListConnectorConfig.BlockingTimeoutMs, ConfigType.Int, RedisListConnectorConfig.DefaultBlockingTimeoutMs, Importance.Low, "Blocking timeout in milliseconds")
        .Define(RedisListConnectorConfig.BatchSize, ConfigType.Int, RedisListConnectorConfig.DefaultBatchSize, Importance.Low, "Maximum items per poll");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
