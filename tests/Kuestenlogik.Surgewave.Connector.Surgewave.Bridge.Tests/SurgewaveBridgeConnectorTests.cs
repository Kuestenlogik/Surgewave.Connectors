namespace Kuestenlogik.Surgewave.Connector.Surgewave.Bridge.Tests;

/// <summary>
/// Configuration validation for the bridge connectors. A replication job that starts with an
/// incomplete configuration would run, produce nothing and look healthy while doing it.
/// </summary>
public class SurgewaveBridgeConnectorTests
{
    [Fact]
    public void SourceConnector_Start_RequiresTheSourceCluster()
    {
        using var connector = new SurgewaveBridgeSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SurgewaveBridgeConnectorConfig.Topic] = "${source.topic}",
            [SurgewaveBridgeConnectorConfig.Topics] = "orders"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SurgewaveBridgeConnectorConfig.SourceBootstrapServers, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RequiresEitherATopicListOrAPattern()
    {
        using var connector = new SurgewaveBridgeSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SurgewaveBridgeConnectorConfig.SourceBootstrapServers] = "localhost:9092",
            [SurgewaveBridgeConnectorConfig.Topic] = "${source.topic}"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SurgewaveBridgeConnectorConfig.TopicsPattern, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_AcceptsAPatternWithoutAnExplicitTopicList()
    {
        using var connector = new SurgewaveBridgeSourceConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SurgewaveBridgeConnectorConfig.SourceBootstrapServers] = "localhost:9092",
            [SurgewaveBridgeConnectorConfig.Topic] = "${source.topic}",
            [SurgewaveBridgeConnectorConfig.TopicsPattern] = "^orders\\..*$"
        };

        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("^orders\\..*$", taskConfig[SurgewaveBridgeConnectorConfig.TopicsPattern]);
    }

    [Fact]
    public void SinkConnector_Start_RequiresTheTopicsToConsume()
    {
        using var connector = new SurgewaveBridgeSinkConnector();

        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SurgewaveBridgeConnectorConfig.TargetBootstrapServers] = "localhost:9092"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SurgewaveBridgeConnectorConfig.Topics, ex.Message, StringComparison.Ordinal);
    }
}
