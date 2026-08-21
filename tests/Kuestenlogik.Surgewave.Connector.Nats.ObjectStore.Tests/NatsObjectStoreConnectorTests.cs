using System.Globalization;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Nats.ObjectStore.Tests;

/// <summary>
/// Covers the configuration contract both Object Store connectors publish and the task-configuration
/// handout.
/// </summary>
public class NatsObjectStoreConnectorTests
{
    [Theory]
    [InlineData(NatsObjectStoreConnectorConfig.Topic)]
    [InlineData(NatsObjectStoreConnectorConfig.BucketName)]
    public void SourceConnector_Start_RequiresTheTopicAndTheBucket(string missingKey)
    {
        using var connector = new NatsObjectStoreSourceConnector();

        var config = SourceConfig();
        config[missingKey] = "  ";

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(NatsObjectStoreConnectorConfig.Topics)]
    [InlineData(NatsObjectStoreConnectorConfig.BucketName)]
    public void SinkConnector_Start_RequiresTheTopicsAndTheBucket(string missingKey)
    {
        using var connector = new NatsObjectStoreSinkConnector();

        var config = SinkConfig();
        config.Remove(missingKey);

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_Config_DeclaresTheChunkSizeTheSinkTaskHonours()
    {
        using var connector = new NatsObjectStoreSinkConnector();

        var chunkSize = Assert.Single(connector.Config.Keys, k => k.Name == NatsObjectStoreConnectorConfig.ChunkSize);

        Assert.Equal(ConfigType.Int, chunkSize.Type);
        Assert.Equal(
            NatsObjectStoreConnectorConfig.DefaultChunkSize.ToString(CultureInfo.InvariantCulture),
            chunkSize.DefaultValue);
    }

    [Fact]
    public void SourceConnector_HandsOutOneIndependentTaskConfig()
    {
        using var connector = new NatsObjectStoreSourceConnector();
        connector.Start(SourceConfig());

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        taskConfig[NatsObjectStoreConnectorConfig.BucketName] = "tampered";

        var second = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("assets", second[NatsObjectStoreConnectorConfig.BucketName]);
        Assert.Equal(typeof(NatsObjectStoreSourceTask), connector.TaskClass);
    }

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [NatsObjectStoreConnectorConfig.Topic] = "objectstore-events",
        [NatsObjectStoreConnectorConfig.BucketName] = "assets",
        [NatsObjectStoreConnectorConfig.Servers] = "nats://localhost:4222"
    };

    private static Dictionary<string, string> SinkConfig() => new(StringComparer.Ordinal)
    {
        [NatsObjectStoreConnectorConfig.Topics] = "objectstore-out",
        [NatsObjectStoreConnectorConfig.BucketName] = "assets",
        [NatsObjectStoreConnectorConfig.Servers] = "nats://localhost:4222"
    };
}
