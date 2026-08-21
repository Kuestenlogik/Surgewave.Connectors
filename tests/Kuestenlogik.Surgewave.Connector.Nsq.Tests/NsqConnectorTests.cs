using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Nsq.Tests;

/// <summary>
/// Covers the configuration contract both NSQ connectors publish: what has to be present before a
/// task is handed out, and that the handed-out configuration cannot be mutated from the outside.
/// </summary>
public class NsqConnectorTests
{
    [Theory]
    [InlineData(NsqConnectorConfig.NsqTopic)]
    [InlineData(NsqConnectorConfig.Topic)]
    public void SourceConnector_Start_RequiresTheTopicsOnBothSides(string missingKey)
    {
        using var connector = new NsqSourceConnector();

        var config = SourceConfig();
        config.Remove(missingKey);

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RequiresANsqdOrLookupdAddress()
    {
        using var connector = new NsqSourceConnector();

        var config = SourceConfig();
        config.Remove(NsqConnectorConfig.NsqdAddress);

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(NsqConnectorConfig.NsqLookupdAddresses, error.Message, StringComparison.Ordinal);

        // Discovery through nsqlookupd alone is a complete configuration.
        config[NsqConnectorConfig.NsqLookupdAddresses] = "127.0.0.1:4161";
        connector.Start(config);

        Assert.Single(connector.TaskConfigs(1));
    }

    [Theory]
    [InlineData(NsqConnectorConfig.NsqdAddress)]
    [InlineData(NsqConnectorConfig.NsqTopic)]
    [InlineData(NsqConnectorConfig.Topics)]
    public void SinkConnector_Start_RequiresTheAddressAndBothTopics(string missingKey)
    {
        using var connector = new NsqSinkConnector();

        var config = SinkConfig();
        config.Remove(missingKey);

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_HandsOutOneIndependentTaskConfig()
    {
        using var connector = new NsqSourceConnector();
        connector.Start(SourceConfig());

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        taskConfig[NsqConnectorConfig.NsqTopic] = "tampered";

        var second = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("orders", second[NsqConnectorConfig.NsqTopic]);
        Assert.Equal(typeof(NsqSourceTask), connector.TaskClass);
    }

    [Fact]
    public void SourceConnector_Config_DefaultsTheChannelAndBatching()
    {
        using var connector = new NsqSourceConnector();
        var keys = connector.Config.Keys;

        Assert.Equal(
            NsqConnectorConfig.DefaultChannel,
            Assert.Single(keys, k => k.Name == NsqConnectorConfig.NsqChannel).DefaultValue);
        Assert.Equal(
            NsqConnectorConfig.DefaultBatchSize,
            Assert.Single(keys, k => k.Name == NsqConnectorConfig.BatchSize).DefaultValue);
        Assert.Contains(keys, k => k.Name == NsqConnectorConfig.AuthSecret && k.Type == ConfigType.Password);
    }

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [NsqConnectorConfig.NsqdAddress] = "127.0.0.1:4150",
        [NsqConnectorConfig.NsqTopic] = "orders",
        [NsqConnectorConfig.Topic] = "nsq-events"
    };

    private static Dictionary<string, string> SinkConfig() => new(StringComparer.Ordinal)
    {
        [NsqConnectorConfig.NsqdAddress] = "127.0.0.1:4150",
        [NsqConnectorConfig.NsqTopic] = "orders",
        [NsqConnectorConfig.Topics] = "nsq-out"
    };
}
