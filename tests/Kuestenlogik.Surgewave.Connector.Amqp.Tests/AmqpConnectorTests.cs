namespace Kuestenlogik.Surgewave.Connector.Amqp.Tests;

/// <summary>
/// Configuration validation and declared options of the AMQP source and sink connectors.
/// </summary>
public class AmqpConnectorTests
{
    [Fact]
    public void SourceConnector_StartRejectsAMissingTopic()
    {
        using var connector = new AmqpSourceConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [AmqpConnectorConfig.SourceQueue] = "orders" }));

        Assert.Contains(AmqpConnectorConfig.Topic, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_StartRejectsAMissingQueue()
    {
        using var connector = new AmqpSourceConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [AmqpConnectorConfig.Topic] = "amqp-events" }));

        Assert.Contains(AmqpConnectorConfig.SourceQueue, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_StartRejectsMissingTopics()
    {
        using var connector = new AmqpSinkConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [AmqpConnectorConfig.TargetExchange] = "events" }));

        Assert.Contains(AmqpConnectorConfig.Topics, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void BothConnectors_DeclareTheHeartbeatOptionTheirTasksRead()
    {
        // CreateConnectionFactory reads 'amqp.heartbeat.seconds', so it has to be a declared
        // option - otherwise the editor never offers it and the value never reaches the task.
        using var source = new AmqpSourceConnector();
        using var sink = new AmqpSinkConnector();

        Assert.Contains(source.Config.Keys, k => k.Name == AmqpConnectorConfig.RequestedHeartbeat);
        Assert.Contains(sink.Config.Keys, k => k.Name == AmqpConnectorConfig.RequestedHeartbeat);
    }

    [Fact]
    public void SourceConnector_HandsTheWholeConfigurationToASingleTask()
    {
        using var connector = new AmqpSourceConnector();
        var config = new Dictionary<string, string>
        {
            [AmqpConnectorConfig.Topic] = "amqp-events",
            [AmqpConnectorConfig.SourceQueue] = "orders",
            [AmqpConnectorConfig.Host] = "broker.internal"
        };

        connector.Start(config);

        // A single consumer cannot be sharded across tasks.
        var taskConfig = Assert.Single(connector.TaskConfigs(4));
        Assert.Equal("broker.internal", taskConfig[AmqpConnectorConfig.Host]);
        Assert.Equal(typeof(AmqpSourceTask), connector.TaskClass);
    }
}
