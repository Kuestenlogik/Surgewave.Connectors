using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Redis.List;

/// <summary>
/// Sink connector for Redis list-based queues using LPUSH/RPUSH.
/// </summary>
[ConnectorMetadata(
    Name = "redis-list-sink",
    Description = "Pushes items to Redis lists",
    Author = "Surgewave",
    Tags = "redis,list,queue,sink")]
public sealed class RedisListSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(RedisListSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(RedisListConnectorConfig.ConnectionString, ConfigType.String, RedisListConnectorConfig.DefaultConnectionString, Importance.High, "Redis connection string")
        .Define(RedisListConnectorConfig.Key, ConfigType.String, Importance.High, "Redis list key to push to")
        .Define(RedisListConnectorConfig.PushDirection, ConfigType.String, RedisListConnectorConfig.DefaultPushDirection, Importance.Medium, "Push direction: left (LPUSH) or right (RPUSH)", EditorHint.Select, options: ["left", "right"]);

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
