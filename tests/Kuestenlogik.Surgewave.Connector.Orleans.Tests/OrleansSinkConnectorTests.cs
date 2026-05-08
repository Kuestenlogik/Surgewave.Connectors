using Kuestenlogik.Surgewave.Connector.Orleans;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Orleans.Tests;

public class OrleansSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new OrleansSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsOrleansSinkTask()
    {
        var connector = new OrleansSinkConnector();
        Assert.Equal(typeof(OrleansSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new OrleansSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.Topics);
        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.ClusterUrl);
        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.ClusterId);
        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.ServiceId);
        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.StreamProvider);
        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.StreamNamespace);
        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.StreamId);
        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.SerializationType);
        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.PublishTimeoutMs);
        Assert.Contains(connector.Config.Keys, k => k.Name == OrleansConnectorConfig.Retries);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new OrleansSinkConnector();
        var config = new Dictionary<string, string>
        {
            [OrleansConnectorConfig.Topics] = "test-topic",
            [OrleansConnectorConfig.StreamNamespace] = "test-namespace"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingTopics_ThrowsArgumentException()
    {
        var connector = new OrleansSinkConnector();
        var config = new Dictionary<string, string>
        {
            [OrleansConnectorConfig.StreamNamespace] = "test-namespace"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingStreamNamespace_ThrowsArgumentException()
    {
        var connector = new OrleansSinkConnector();
        var config = new Dictionary<string, string>
        {
            [OrleansConnectorConfig.Topics] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new OrleansSinkConnector();
        var config = new Dictionary<string, string>
        {
            [OrleansConnectorConfig.Topics] = "test-topic",
            [OrleansConnectorConfig.StreamNamespace] = "test-namespace"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("test-namespace", taskConfigs[0][OrleansConnectorConfig.StreamNamespace]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new OrleansSinkConnector();
        var config = new Dictionary<string, string>
        {
            [OrleansConnectorConfig.Topics] = "test-topic",
            [OrleansConnectorConfig.StreamNamespace] = "test-namespace"
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
