using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Redis.Scan.Tests;

/// <summary>
/// Covers the configuration contract the connector publishes to the worker and the UI, plus the
/// task-configuration handout.
/// </summary>
public class RedisScanSourceConnectorTests
{
    [Fact]
    public void Config_DeclaresTheScanContract()
    {
        using var connector = new RedisScanSourceConnector();
        var keys = connector.Config.Keys;

        Assert.Contains(keys, k => k.Name == RedisScanConnectorConfig.Topic && k.Type == ConfigType.String);
        Assert.Contains(keys, k => k.Name == RedisScanConnectorConfig.ConnectionString && k.Type == ConfigType.String);
        Assert.Contains(keys, k => k.Name == RedisScanConnectorConfig.Pattern && k.Type == ConfigType.String);
        Assert.Contains(keys, k => k.Name == RedisScanConnectorConfig.BatchSize && k.Type == ConfigType.Int);
        Assert.Contains(keys, k => k.Name == RedisScanConnectorConfig.PollIntervalMs && k.Type == ConfigType.Int);
        Assert.Contains(keys, k => k.Name == RedisScanConnectorConfig.Database && k.Type == ConfigType.Int);
    }

    [Fact]
    public void Config_KeyTypeFilterOffersTheRedisTypes()
    {
        using var connector = new RedisScanSourceConnector();

        var key = Assert.Single(connector.Config.Keys, k => k.Name == RedisScanConnectorConfig.KeyType);

        Assert.Equal(new[] { "string", "list", "set", "zset", "hash" }, key.Options);
        // The empty default means "do not filter by type".
        Assert.Equal(RedisScanConnectorConfig.DefaultKeyType, key.DefaultValue);
    }

    [Fact]
    public void Config_IncludeValueIsAdvertisedAsOnByDefault()
    {
        using var connector = new RedisScanSourceConnector();

        var key = Assert.Single(connector.Config.Keys, k => k.Name == RedisScanConnectorConfig.IncludeValue);

        Assert.Equal(ConfigType.Boolean, key.Type);
        Assert.True(Assert.IsType<bool>(key.DefaultValue));
    }

    [Fact]
    public void Config_ScansEverythingByDefault()
    {
        using var connector = new RedisScanSourceConnector();

        var key = Assert.Single(connector.Config.Keys, k => k.Name == RedisScanConnectorConfig.Pattern);

        Assert.Equal("*", key.DefaultValue);
    }

    [Fact]
    public void TaskConfigs_HandsOutAnIndependentCopy()
    {
        using var connector = new RedisScanSourceConnector();
        connector.Start(new Dictionary<string, string>
        {
            [RedisScanConnectorConfig.Topic] = "redis-keys",
            [RedisScanConnectorConfig.Pattern] = "user:*"
        });

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        taskConfig[RedisScanConnectorConfig.Pattern] = "tampered:*";

        var second = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("user:*", second[RedisScanConnectorConfig.Pattern]);
    }

    [Fact]
    public void Connector_PointsAtItsTaskType()
    {
        using var connector = new RedisScanSourceConnector();

        Assert.Equal(typeof(RedisScanSourceTask), connector.TaskClass);
    }
}
