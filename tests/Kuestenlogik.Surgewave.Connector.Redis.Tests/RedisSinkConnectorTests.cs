namespace Kuestenlogik.Surgewave.Connector.Redis.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class RedisSinkConnectorTests
{
    [Fact]
    public void RedisSinkConnector_HasCorrectVersion()
    {
        using var connector = new RedisSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void RedisSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new RedisSinkConnector();
        Assert.Equal(typeof(RedisSinkTask), connector.TaskClass);
    }

    [Fact]
    public void RedisSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new RedisSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "redis.connection" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "topics" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "redis.mode" && k.Type == ConfigType.String);
    }

    [Fact]
    public void RedisSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new RedisSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "redis.key.prefix");
        Assert.Contains(config.Keys, k => k.Name == "redis.ttl.seconds");
        Assert.Contains(config.Keys, k => k.Name == "batch.size");
        Assert.Contains(config.Keys, k => k.Name == "redis.hash.key.field");
        Assert.Contains(config.Keys, k => k.Name == "redis.stream.name");
        Assert.Contains(config.Keys, k => k.Name == "retry.max");
        Assert.Contains(config.Keys, k => k.Name == "retry.backoff.ms");
    }

    [Fact]
    public void RedisSinkConnector_Start_ThrowsOnMissingConnection()
    {
        using var connector = new RedisSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("redis.connection", ex.Message);
    }

    [Fact]
    public void RedisSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new RedisSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Theory]
    [InlineData("string")]
    [InlineData("hash")]
    [InlineData("stream")]
    public void RedisSinkConnector_Start_AcceptsValidModes(string mode)
    {
        using var connector = new RedisSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topics"] = "test-topic",
            ["redis.mode"] = mode
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void RedisSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new RedisSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topics"] = "test-topic",
            ["redis.mode"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void RedisSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new RedisSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topics"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("localhost:6379", taskConfigs[0]["redis.connection"]);
        Assert.Equal("test-topic", taskConfigs[0]["topics"]);
    }

    [Fact]
    public void RedisSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new RedisSinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == "redis.mode");
        Assert.Equal("string", modeKey.DefaultValue);

        var batchKey = config.Keys.First(k => k.Name == "batch.size");
        Assert.Equal(100L, batchKey.DefaultValue);

        var retryMaxKey = config.Keys.First(k => k.Name == "retry.max");
        Assert.Equal(3L, retryMaxKey.DefaultValue);

        var streamNameKey = config.Keys.First(k => k.Name == "redis.stream.name");
        Assert.Equal("${topic}", streamNameKey.DefaultValue);
    }

    private static ConnectorContext CreateContext()
    {
        return new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { },
            Logger = null
        };
    }
}
