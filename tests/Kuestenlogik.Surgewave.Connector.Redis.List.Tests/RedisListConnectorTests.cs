namespace Kuestenlogik.Surgewave.Connector.Redis.List.Tests;

/// <summary>
/// Covers the task-configuration handout and the declared configuration contract of both
/// Redis list connectors.
/// </summary>
public class RedisListConnectorTests
{
    [Fact]
    public void SourceConnector_TaskConfigs_HandsOutAnIndependentCopy()
    {
        using var connector = new RedisListSourceConnector();
        connector.Start(SourceConfig());

        var handedOut = connector.TaskConfigs(4);
        var taskConfig = Assert.Single(handedOut);
        taskConfig[RedisListConnectorConfig.Key] = "tampered";

        // A task that rewrites its own copy must not corrupt the connector's configuration.
        var second = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("orders", second[RedisListConnectorConfig.Key]);
    }

    [Fact]
    public void SinkConnector_TaskConfigs_IsAlwaysOneTask()
    {
        using var connector = new RedisListSinkConnector();
        connector.Start(new Dictionary<string, string>
        {
            [RedisListConnectorConfig.Key] = "orders",
            [RedisListConnectorConfig.PushDirection] = "left"
        });

        var taskConfig = Assert.Single(connector.TaskConfigs(8));
        Assert.Equal("left", taskConfig[RedisListConnectorConfig.PushDirection]);
    }

    [Fact]
    public void SourceConnector_Config_DeclaresBothPopDirections()
    {
        using var connector = new RedisListSourceConnector();

        var key = Assert.Single(connector.Config.Keys, k => k.Name == RedisListConnectorConfig.PopDirection);

        Assert.Equal(new[] { "left", "right" }, key.Options);
        Assert.Equal(RedisListConnectorConfig.DefaultPopDirection, key.DefaultValue);
    }

    [Fact]
    public void SourceConnector_Config_RequiresKeyAndTopic()
    {
        using var connector = new RedisListSourceConnector();
        var names = connector.Config.Keys.Select(k => k.Name).ToList();

        Assert.Contains(RedisListConnectorConfig.Key, names);
        Assert.Contains(RedisListConnectorConfig.Topic, names);
        Assert.Contains(RedisListConnectorConfig.ConnectionString, names);
    }

    [Fact]
    public void Connectors_PointAtTheirTaskTypes()
    {
        using var source = new RedisListSourceConnector();
        using var sink = new RedisListSinkConnector();

        Assert.Equal(typeof(RedisListSourceTask), source.TaskClass);
        Assert.Equal(typeof(RedisListSinkTask), sink.TaskClass);
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [RedisListConnectorConfig.ConnectionString] = "localhost:6379",
        [RedisListConnectorConfig.Key] = "orders",
        [RedisListConnectorConfig.Topic] = "orders-topic"
    };
}
