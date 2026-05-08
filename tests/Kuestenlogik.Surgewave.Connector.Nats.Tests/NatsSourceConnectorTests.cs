using Kuestenlogik.Surgewave.Connector.Nats;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats.Tests;

public class NatsSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new NatsSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsNatsSourceTask()
    {
        var connector = new NatsSourceConnector();
        Assert.Equal(typeof(NatsSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new NatsSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.Url);
        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.StreamName);
        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.Topic);
        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.ConsumerName);
        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.FetchBatchSize);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new NatsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingStreamName_ThrowsArgumentException()
    {
        var connector = new NatsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Topic] = "test-topic",
            [NatsConnectorConfig.ConsumerName] = "test-consumer"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingTopic_ThrowsArgumentException()
    {
        var connector = new NatsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingConsumerName_ThrowsArgumentException()
    {
        var connector = new NatsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.Topic] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithDeliverPolicy_Succeeds()
    {
        var connector = new NatsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic",
            [NatsConnectorConfig.DeliverPolicy] = "new"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithAckPolicy_Succeeds()
    {
        var connector = new NatsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic",
            [NatsConnectorConfig.AckPolicy] = "none"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new NatsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("test-stream", taskConfigs[0][NatsConnectorConfig.StreamName]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new NatsSourceConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.StreamName] = "test-stream",
            [NatsConnectorConfig.ConsumerName] = "test-consumer",
            [NatsConnectorConfig.Topic] = "test-topic"
        };
        connector.Start(config);

        var exception = Record.Exception(() =>
        {
            connector.Stop();
            connector.Stop();
        });

        Assert.Null(exception);
    }
}
