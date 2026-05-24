using Kuestenlogik.Surgewave.Connector.Nats.Kv;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats.Kv.Tests;

/// <summary>
/// Tests for NATS KV source and sink connectors.
/// </summary>
public sealed class NatsKvConnectorTests
{
    [Fact]
    public void NatsKvSourceConnector_HasCorrectVersion()
    {
        var connector = new NatsKvSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void NatsKvSinkConnector_HasCorrectVersion()
    {
        var connector = new NatsKvSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void NatsKvConnectorConfig_HasExpectedConstants()
    {
        Assert.Equal("nats.url", NatsKvConnectorConfig.Url);
        Assert.Equal("nats://localhost:4222", NatsKvConnectorConfig.DefaultUrl);
        Assert.Equal("kv.bucket", NatsKvConnectorConfig.Bucket);
        Assert.Equal("kv.key.pattern", NatsKvConnectorConfig.KeyPattern);
        Assert.Equal("*", NatsKvConnectorConfig.DefaultKeyPattern);
        Assert.Equal("kv.watch.mode", NatsKvConnectorConfig.WatchMode);
        Assert.Equal("all", NatsKvConnectorConfig.WatchAll);
        Assert.Equal("updates", NatsKvConnectorConfig.WatchUpdatesOnly);
        Assert.Equal("kv.write.mode", NatsKvConnectorConfig.WriteMode);
        Assert.Equal("put", NatsKvConnectorConfig.WriteModeUpsert);
        Assert.Equal("create", NatsKvConnectorConfig.WriteModeCreate);
        Assert.Equal("update", NatsKvConnectorConfig.WriteModeUpdate);
        Assert.Equal("delete", NatsKvConnectorConfig.WriteModeDelete);
    }

    [Fact]
    public void NatsKvSourceConnector_ThrowsOnMissingTopic()
    {
        var connector = new NatsKvSourceConnector();
        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void NatsKvSourceConnector_ThrowsOnMissingBucket()
    {
        var connector = new NatsKvSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsKvConnectorConfig.Topic] = "test"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void NatsKvSourceConnector_ProducesTaskConfigs()
    {
        var connector = new NatsKvSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsKvConnectorConfig.Topic] = "test-topic",
            [NatsKvConnectorConfig.Bucket] = "test-bucket",
            [NatsKvConnectorConfig.Url] = "nats://test:4222"
        };

        connector.Start(config);
        var configs = connector.TaskConfigs(1);

        Assert.Single(configs);
        Assert.Equal("test-topic", configs[0][NatsKvConnectorConfig.Topic]);
        Assert.Equal("test-bucket", configs[0][NatsKvConnectorConfig.Bucket]);
        Assert.Equal("nats://test:4222", configs[0][NatsKvConnectorConfig.Url]);
    }

    [Fact]
    public void NatsKvSinkConnector_ThrowsOnMissingTopics()
    {
        var connector = new NatsKvSinkConnector();
        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void NatsKvSinkConnector_ThrowsOnMissingBucket()
    {
        var connector = new NatsKvSinkConnector();
        var config = new Dictionary<string, string>
        {
            [NatsKvConnectorConfig.Topics] = "test"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void NatsKvSinkConnector_ProducesTaskConfigs()
    {
        var connector = new NatsKvSinkConnector();
        var config = new Dictionary<string, string>
        {
            [NatsKvConnectorConfig.Topics] = "test-topic",
            [NatsKvConnectorConfig.Bucket] = "test-bucket",
            [NatsKvConnectorConfig.WriteMode] = NatsKvConnectorConfig.WriteModeDelete
        };

        connector.Start(config);
        var configs = connector.TaskConfigs(1);

        Assert.Single(configs);
        Assert.Equal("test-topic", configs[0][NatsKvConnectorConfig.Topics]);
        Assert.Equal("test-bucket", configs[0][NatsKvConnectorConfig.Bucket]);
        Assert.Equal(NatsKvConnectorConfig.WriteModeDelete, configs[0][NatsKvConnectorConfig.WriteMode]);
    }

    [Fact]
    public void NatsKvSourceConnector_HasCorrectTaskClass()
    {
        var connector = new NatsKvSourceConnector();
        Assert.Equal(typeof(NatsKvSourceTask), connector.TaskClass);
    }

    [Fact]
    public void NatsKvSinkConnector_HasCorrectTaskClass()
    {
        var connector = new NatsKvSinkConnector();
        Assert.Equal(typeof(NatsKvSinkTask), connector.TaskClass);
    }

    [Fact]
    public void NatsKvSourceConnector_HasConfig()
    {
        var connector = new NatsKvSourceConnector();
        var configDef = connector.Config;
        Assert.NotNull(configDef);
        Assert.True(configDef.Keys.Count > 0);
    }

    [Fact]
    public void NatsKvSinkConnector_HasConfig()
    {
        var connector = new NatsKvSinkConnector();
        var configDef = connector.Config;
        Assert.NotNull(configDef);
        Assert.True(configDef.Keys.Count > 0);
    }

    [Fact]
    public void NatsKvSourceConnector_StopsCleanly()
    {
        var connector = new NatsKvSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsKvConnectorConfig.Topic] = "test",
            [NatsKvConnectorConfig.Bucket] = "test-bucket"
        };

        connector.Start(config);
        connector.Stop();
        // Should not throw
    }

    [Fact]
    public void NatsKvSinkConnector_StopsCleanly()
    {
        var connector = new NatsKvSinkConnector();
        var config = new Dictionary<string, string>
        {
            [NatsKvConnectorConfig.Topics] = "test",
            [NatsKvConnectorConfig.Bucket] = "test-bucket"
        };

        connector.Start(config);
        connector.Stop();
        // Should not throw
    }

    [Fact]
    public void NatsKvSourceConnector_WithAllOptions()
    {
        var connector = new NatsKvSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsKvConnectorConfig.Topic] = "test",
            [NatsKvConnectorConfig.Bucket] = "test-bucket",
            [NatsKvConnectorConfig.Url] = "nats://test:4222",
            [NatsKvConnectorConfig.KeyPattern] = "prefix.*",
            [NatsKvConnectorConfig.WatchMode] = NatsKvConnectorConfig.WatchUpdatesOnly,
            [NatsKvConnectorConfig.IncludeHistory] = "true",
            [NatsKvConnectorConfig.CreateBucketIfMissing] = "false",
            [NatsKvConnectorConfig.Username] = "user",
            [NatsKvConnectorConfig.Password] = "pass"
        };

        connector.Start(config);
        var configs = connector.TaskConfigs(1);

        Assert.Single(configs);
        Assert.Equal("prefix.*", configs[0][NatsKvConnectorConfig.KeyPattern]);
        Assert.Equal(NatsKvConnectorConfig.WatchUpdatesOnly, configs[0][NatsKvConnectorConfig.WatchMode]);
        Assert.Equal("True", configs[0][NatsKvConnectorConfig.IncludeHistory]);
        Assert.Equal("False", configs[0][NatsKvConnectorConfig.CreateBucketIfMissing]);
    }

    [Fact]
    public void NatsKvSinkConnector_WithAllOptions()
    {
        var connector = new NatsKvSinkConnector();
        var config = new Dictionary<string, string>
        {
            [NatsKvConnectorConfig.Topics] = "test",
            [NatsKvConnectorConfig.Bucket] = "test-bucket",
            [NatsKvConnectorConfig.Url] = "nats://test:4222",
            [NatsKvConnectorConfig.KeyField] = "id",
            [NatsKvConnectorConfig.WriteMode] = NatsKvConnectorConfig.WriteModeCreate,
            [NatsKvConnectorConfig.History] = "10",
            [NatsKvConnectorConfig.Ttl] = "3600",
            [NatsKvConnectorConfig.Token] = "secret-token"
        };

        connector.Start(config);
        var configs = connector.TaskConfigs(1);

        Assert.Single(configs);
        Assert.Equal("id", configs[0][NatsKvConnectorConfig.KeyField]);
        Assert.Equal(NatsKvConnectorConfig.WriteModeCreate, configs[0][NatsKvConnectorConfig.WriteMode]);
        Assert.Equal("10", configs[0][NatsKvConnectorConfig.History]);
        Assert.Equal("3600", configs[0][NatsKvConnectorConfig.Ttl]);
    }
}
