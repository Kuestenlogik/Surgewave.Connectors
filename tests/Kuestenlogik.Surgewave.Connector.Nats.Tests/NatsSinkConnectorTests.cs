using Kuestenlogik.Surgewave.Connector.Nats;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nats.Tests;

public class NatsSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new NatsSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsNatsSinkTask()
    {
        var connector = new NatsSinkConnector();
        Assert.Equal(typeof(NatsSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new NatsSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.Url);
        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.Topics);
        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.StreamName);
        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.Username);
        Assert.Contains(connector.Config.Keys, k => k.Name == NatsConnectorConfig.Password);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new NatsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Topics] = "test-topic",
            [NatsConnectorConfig.StreamName] = "test-stream"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingTopics_ThrowsArgumentException()
    {
        var connector = new NatsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Url] = "nats://localhost:4222",
            [NatsConnectorConfig.StreamName] = "test-stream"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingStreamName_ThrowsArgumentException()
    {
        var connector = new NatsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Topics] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new NatsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Topics] = "test-topic",
            [NatsConnectorConfig.StreamName] = "test-stream"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("test-stream", taskConfigs[0][NatsConnectorConfig.StreamName]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new NatsSinkConnector();
        var config = new Dictionary<string, string>
        {
            [NatsConnectorConfig.Topics] = "test-topic",
            [NatsConnectorConfig.StreamName] = "test-stream"
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
