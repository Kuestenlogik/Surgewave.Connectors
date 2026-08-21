namespace Kuestenlogik.Surgewave.Connector.Kafka.Bridge.Tests;

/// <summary>
/// Validation performed before any Kafka client is built - a misconfigured bridge must fail on
/// <c>Start</c> rather than at the first poll.
/// </summary>
public class KafkaBridgeConnectorTests
{
    [Fact]
    public void SourceConnector_Start_RequiresBootstrapServers()
    {
        using var connector = new KafkaBridgeSourceConnector();
        var config = SourceConfig();
        config.Remove(KafkaBridgeConnectorConfig.KafkaBootstrapServers);

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(KafkaBridgeConnectorConfig.KafkaBootstrapServers, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RequiresDestinationTopic()
    {
        using var connector = new KafkaBridgeSourceConnector();
        var config = SourceConfig();
        config[KafkaBridgeConnectorConfig.Topic] = "   ";

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(KafkaBridgeConnectorConfig.Topic, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_Start_RequiresTopicsOrPattern()
    {
        using var connector = new KafkaBridgeSourceConnector();
        var config = SourceConfig();
        config.Remove(KafkaBridgeConnectorConfig.Topics);

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(KafkaBridgeConnectorConfig.TopicsPattern, error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_TaskConfigs_HandsTheTaskAnIndependentCopy()
    {
        using var connector = new KafkaBridgeSourceConnector();
        var config = SourceConfig();
        connector.Start(config);

        var taskConfig = Assert.Single(connector.TaskConfigs(3));
        config[KafkaBridgeConnectorConfig.Topics] = "changed-after-start";

        Assert.Equal(typeof(KafkaBridgeSourceTask), connector.TaskClass);
        Assert.Equal("orders", taskConfig[KafkaBridgeConnectorConfig.Topics]);
    }

    [Fact]
    public void SinkConnector_Start_RequiresTopics()
    {
        using var connector = new KafkaBridgeSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KafkaBridgeConnectorConfig.KafkaBootstrapServers] = "localhost:9092"
        };

        var error = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(KafkaBridgeConnectorConfig.Topics, error.Message, StringComparison.Ordinal);
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [KafkaBridgeConnectorConfig.KafkaBootstrapServers] = "localhost:9092",
        [KafkaBridgeConnectorConfig.Topic] = "sw-orders",
        [KafkaBridgeConnectorConfig.Topics] = "orders"
    };
}
