using Xunit;
using Kuestenlogik.Surgewave.Connector.RabbitMQ;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RabbitMQ.Tests;

public class RabbitMQSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new RabbitMQSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsRabbitMQSinkTask()
    {
        var connector = new RabbitMQSinkConnector();
        Assert.Equal(typeof(RabbitMQSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new RabbitMQSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Host);
        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Topics);
        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Exchange);
        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.RoutingKeyTemplate);
        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Username);
        Assert.Contains(connector.Config.Keys, k => k.Name == RabbitMQConnectorConfig.Password);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new RabbitMQSinkConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingTopics_ThrowsArgumentException()
    {
        var connector = new RabbitMQSinkConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Host] = "localhost"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithExchangeConfig_Succeeds()
    {
        var connector = new RabbitMQSinkConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.Exchange] = "my-exchange",
            [RabbitMQConnectorConfig.ExchangeType] = "topic"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithRoutingKeyTemplate_Succeeds()
    {
        var connector = new RabbitMQSinkConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.RoutingKeyTemplate] = "events.${topic}.${key}"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new RabbitMQSinkConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.RoutingKeyTemplate] = "custom.${topic}"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("custom.${topic}", taskConfigs[0][RabbitMQConnectorConfig.RoutingKeyTemplate]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new RabbitMQSinkConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic"
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
    public void Start_WithPersistenceConfig_Succeeds()
    {
        var connector = new RabbitMQSinkConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.Persistent] = "true",
            [RabbitMQConnectorConfig.Mandatory] = "true"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMessageTtl_Succeeds()
    {
        var connector = new RabbitMQSinkConnector();
        var config = new Dictionary<string, string>
        {
            [RabbitMQConnectorConfig.Topics] = "test-topic",
            [RabbitMQConnectorConfig.MessageTtlMs] = "60000"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }
}
