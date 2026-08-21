using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Pulsar.Tests;

/// <summary>
/// Covers the configuration contract both Pulsar connectors publish, including the source's
/// "either a topic list or a topic pattern" rule.
/// </summary>
public class PulsarConnectorTests
{
    [Fact]
    public void SourceConnector_Start_RequiresTheSurgewaveTopic()
    {
        using var connector = new PulsarSourceConnector();

        var config = SourceConfig();
        config[PulsarConnectorConfig.Topic] = "   ";

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(PulsarConnectorConfig.Topic, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RejectsAConfigWithNeitherTopicsNorPattern()
    {
        using var connector = new PulsarSourceConnector();

        var config = SourceConfig();
        config.Remove(PulsarConnectorConfig.Topics);

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(PulsarConnectorConfig.TopicsPattern, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_AcceptsATopicPatternInsteadOfATopicList()
    {
        using var connector = new PulsarSourceConnector();

        var config = SourceConfig();
        config.Remove(PulsarConnectorConfig.Topics);
        config[PulsarConnectorConfig.TopicsPattern] = "persistent://public/default/orders-.*";

        connector.Start(config);

        Assert.Single(connector.TaskConfigs(1));
    }

    [Theory]
    [InlineData(PulsarConnectorConfig.Topic)]
    [InlineData(PulsarConnectorConfig.Topics)]
    public void SinkConnector_Start_RequiresBothTopicSides(string missingKey)
    {
        using var connector = new PulsarSinkConnector();

        var config = SinkConfig();
        config.Remove(missingKey);

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(missingKey, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_HandsOutOneIndependentTaskConfig()
    {
        using var connector = new PulsarSourceConnector();
        connector.Start(SourceConfig());

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        taskConfig[PulsarConnectorConfig.Topics] = "tampered";

        var second = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("persistent://public/default/orders", second[PulsarConnectorConfig.Topics]);
        Assert.Equal(typeof(PulsarSourceTask), connector.TaskClass);
    }

    [Fact]
    public void SourceConnector_Config_OffersTheSubscriptionTypesTheTaskUnderstands()
    {
        using var connector = new PulsarSourceConnector();

        var subscriptionType = Assert.Single(
            connector.Config.Keys,
            k => k.Name == PulsarConnectorConfig.SubscriptionType);

        Assert.Equal(PulsarConnectorConfig.DefaultSubscriptionType, subscriptionType.DefaultValue);
        Assert.Equal(
            new[] { "Exclusive", "Shared", "Failover", "Key_Shared" },
            Assert.IsType<string[]>(subscriptionType.Options));

        var initialPosition = Assert.Single(connector.Config.Keys, k => k.Name == PulsarConnectorConfig.InitialPosition);
        Assert.Equal(PulsarConnectorConfig.DefaultInitialPosition, initialPosition.DefaultValue);
        Assert.Equal(ConfigType.String, initialPosition.Type);
    }

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [PulsarConnectorConfig.ServiceUrl] = "pulsar://localhost:6650",
        [PulsarConnectorConfig.Topic] = "${pulsar.topic}",
        [PulsarConnectorConfig.Topics] = "persistent://public/default/orders"
    };

    private static Dictionary<string, string> SinkConfig() => new(StringComparer.Ordinal)
    {
        [PulsarConnectorConfig.ServiceUrl] = "pulsar://localhost:6650",
        [PulsarConnectorConfig.Topic] = "persistent://public/default/mirror",
        [PulsarConnectorConfig.Topics] = "orders"
    };
}
