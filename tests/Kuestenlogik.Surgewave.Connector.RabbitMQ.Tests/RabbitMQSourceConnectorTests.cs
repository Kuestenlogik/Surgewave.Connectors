using Xunit;
using Kuestenlogik.Surgewave.Connector.RabbitMQ;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RabbitMQ.Tests;

public class RabbitMQSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new RabbitMQSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsRabbitMQSourceTask()
    {
        var connector = new RabbitMQSourceConnector();
        Assert.Equal(typeof(RabbitMQSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new RabbitMQSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Host);
        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Queue);
        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Topic);
        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Username);
        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Password);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new RabbitMQSourceConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingQueue_ThrowsArgumentException()
    {
        var connector = new RabbitMQSourceConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topic] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingTopic_ThrowsArgumentException()
    {
        var connector = new RabbitMQSourceConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new RabbitMQSourceConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic",
            [RabbitMQConnectorConfig.PrefetchCount] = "200"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("200", taskConfigs[0][RabbitMQConnectorConfig.PrefetchCount]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new RabbitMQSourceConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic"
        };
        connector.Start(config);

        var exception = Record.Exception(() =>
        {
            connector.Stop();
            connector.Stop();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithDeadLetterExchange_Succeeds()
    {
        var connector = new RabbitMQSourceConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Queue] = "test-queue",
            [RabbitMQConnectorConfig.Topic] = "test-topic",
            [RabbitMQConnectorConfig.DeadLetterExchange] = "dlx",
            [RabbitMQConnectorConfig.DeadLetterRoutingKey] = "dlq"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }
}
