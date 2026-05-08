using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Redis.Scan;

/// <summary>
/// Source connector for iterating Redis keys via SCAN with pattern matching and TYPE filtering.
/// </summary>
[ConnectorMetadata(
    Name = "redis-scan-source",
    Description = "Scans Redis keys with pattern matching and optional TYPE filtering",
    Author = "Surgewave",
    Tags = "redis,scan,keys,source")]
public sealed class RedisScanSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(RedisScanSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(RedisScanConnectorConfig.ConnectionString, ConfigType.String, RedisScanConnectorConfig.DefaultConnectionString, Importance.High, "Redis connection string")
        .Define(RedisScanConnectorConfig.Topic, ConfigType.String, Importance.High, "Surgewave topic to write to", EditorHint.Topic)
        .Define(RedisScanConnectorConfig.Pattern, ConfigType.String, RedisScanConnectorConfig.DefaultPattern, Importance.Medium, "Key pattern for SCAN (e.g., 'user:*')")
        .Define(RedisScanConnectorConfig.KeyType, ConfigType.String, RedisScanConnectorConfig.DefaultKeyType, Importance.Low, "Filter by key type: string, list, set, zset, hash (empty for all)", EditorHint.Select, options: ["string", "list", "set", "zset", "hash"])
        .Define(RedisScanConnectorConfig.IncludeValue, ConfigType.Boolean, RedisScanConnectorConfig.DefaultIncludeValue, Importance.Medium, "Include key values in output")
        .Define(RedisScanConnectorConfig.BatchSize, ConfigType.Int, RedisScanConnectorConfig.DefaultBatchSize, Importance.Low, "Maximum keys per scan iteration")
        .Define(RedisScanConnectorConfig.PollIntervalMs, ConfigType.Int, RedisScanConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Interval between full scans in milliseconds")
        .Define(RedisScanConnectorConfig.Database, ConfigType.Int, RedisScanConnectorConfig.DefaultDatabase, Importance.Low, "Redis database number");

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
