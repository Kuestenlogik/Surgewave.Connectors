namespace Kuestenlogik.Surgewave.Connector.Redis.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class RedisSourceConnectorTests
{
    [Fact]
    public void RedisSourceConnector_HasCorrectVersion()
    {
        using var connector = new RedisSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void RedisSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new RedisSourceConnector();
        Assert.Equal(typeof(RedisSourceTask), connector.TaskClass);
    }

    [Fact]
    public void RedisSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new RedisSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "redis.connection" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "topic" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "redis.mode" && k.Type == ConfigType.String);
    }

    [Fact]
    public void RedisSourceConnector_Config_HasOptionalKeys()
    {
        using var connector = new RedisSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "redis.streams");
        Assert.Contains(config.Keys, k => k.Name == "redis.consumer.group");
        Assert.Contains(config.Keys, k => k.Name == "redis.consumer.name");
        Assert.Contains(config.Keys, k => k.Name == "redis.pubsub.channels");
        Assert.Contains(config.Keys, k => k.Name == "poll.interval.ms");
        Assert.Contains(config.Keys, k => k.Name == "batch.max.records");
    }

    [Fact]
    public void RedisSourceConnector_Start_ThrowsOnMissingConnection()
    {
        using var connector = new RedisSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topic"] = "test-topic",
            ["redis.streams"] = "my-stream"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("redis.connection", ex.Message);
    }

    [Fact]
    public void RedisSourceConnector_Start_ThrowsOnMissingTopic()
    {
        using var connector = new RedisSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["redis.streams"] = "my-stream"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topic", ex.Message);
    }

    [Fact]
    public void RedisSourceConnector_Start_ThrowsOnMissingStreamsInStreamMode()
    {
        using var connector = new RedisSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topic"] = "test-topic",
            ["redis.mode"] = "stream"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("redis.streams", ex.Message);
    }

    [Fact]
    public void RedisSourceConnector_Start_ThrowsOnMissingChannelsInPubSubMode()
    {
        using var connector = new RedisSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topic"] = "test-topic",
            ["redis.mode"] = "pubsub"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("redis.pubsub.channels", ex.Message);
    }

    [Theory]
    [InlineData("stream")]
    [InlineData("pubsub")]
    public void RedisSourceConnector_Start_AcceptsValidModes(string mode)
    {
        using var connector = new RedisSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topic"] = "test-topic",
            ["redis.mode"] = mode
        };

        // Add mode-specific required config
        if (mode == "stream")
            config["redis.streams"] = "my-stream";
        else
            config["redis.pubsub.channels"] = "my-channel";

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void RedisSourceConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new RedisSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topic"] = "test-topic",
            ["redis.mode"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void RedisSourceConnector_TaskConfigs_PartitionsStreamsByTask()
    {
        using var connector = new RedisSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topic"] = "test-topic",
            ["redis.mode"] = "stream",
            ["redis.streams"] = "stream1,stream2,stream3"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Equal(3, taskConfigs.Count);
        Assert.Equal("stream1", taskConfigs[0]["redis.streams"]);
        Assert.Equal("stream2", taskConfigs[1]["redis.streams"]);
        Assert.Equal("stream3", taskConfigs[2]["redis.streams"]);
    }

    [Fact]
    public void RedisSourceConnector_TaskConfigs_AssignsUniqueConsumerNames()
    {
        using var connector = new RedisSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topic"] = "test-topic",
            ["redis.mode"] = "stream",
            ["redis.streams"] = "stream1,stream2"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(2);

        Assert.Equal("consumer-0", taskConfigs[0]["redis.consumer.name"]);
        Assert.Equal("consumer-1", taskConfigs[1]["redis.consumer.name"]);
    }

    [Fact]
    public void RedisSourceConnector_TaskConfigs_PubSubReturnsSingleTask()
    {
        using var connector = new RedisSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["redis.connection"] = "localhost:6379",
            ["topic"] = "test-topic",
            ["redis.mode"] = "pubsub",
            ["redis.pubsub.channels"] = "channel1,channel2,channel3"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        // Pub/Sub should return a single task regardless of maxTasks
        Assert.Single(taskConfigs);
    }

    [Fact]
    public void RedisSourceConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new RedisSourceConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == "redis.mode");
        Assert.Equal("stream", modeKey.DefaultValue);

        var consumerGroupKey = config.Keys.First(k => k.Name == "redis.consumer.group");
        Assert.Equal("surgewave-connect", consumerGroupKey.DefaultValue);

        var pollIntervalKey = config.Keys.First(k => k.Name == "poll.interval.ms");
        Assert.Equal(1000L, pollIntervalKey.DefaultValue);

        var batchMaxKey = config.Keys.First(k => k.Name == "batch.max.records");
        Assert.Equal(500L, batchMaxKey.DefaultValue);
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
