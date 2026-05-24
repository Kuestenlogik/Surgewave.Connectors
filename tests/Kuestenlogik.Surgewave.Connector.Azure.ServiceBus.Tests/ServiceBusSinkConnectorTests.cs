namespace Kuestenlogik.Surgewave.Connector.Azure.ServiceBus.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class ServiceBusSinkConnectorTests
{
    [Fact]
    public void ServiceBusSinkConnector_HasCorrectVersion()
    {
        using var connector = new ServiceBusSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void ServiceBusSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new ServiceBusSinkConnector();
        Assert.Equal(typeof(ServiceBusSinkTask), connector.TaskClass);
    }

    [Fact]
    public void ServiceBusSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new ServiceBusSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.connection.string" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "topics" && k.Type == ConfigType.String);
    }

    [Fact]
    public void ServiceBusSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new ServiceBusSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.queue.name");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.topic.name");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.session.id.field");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.partition.key.field");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.batch.size");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.header.prefix");
    }

    [Fact]
    public void ServiceBusSinkConnector_Start_ThrowsOnMissingConnectionString()
    {
        using var connector = new ServiceBusSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topics"] = "surgewave-topic",
            ["azure.servicebus.queue.name"] = "my-queue"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("azure.servicebus.connection.string", ex.Message);
    }

    [Fact]
    public void ServiceBusSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new ServiceBusSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["azure.servicebus.queue.name"] = "my-queue"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void ServiceBusSinkConnector_Start_ThrowsOnMissingQueueAndTopic()
    {
        using var connector = new ServiceBusSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["topics"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Must specify", ex.Message);
    }

    [Fact]
    public void ServiceBusSinkConnector_Start_ThrowsOnBothQueueAndTopic()
    {
        using var connector = new ServiceBusSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["topics"] = "surgewave-topic",
            ["azure.servicebus.queue.name"] = "my-queue",
            ["azure.servicebus.topic.name"] = "my-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mutually exclusive", ex.Message);
    }

    [Fact]
    public void ServiceBusSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new ServiceBusSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["topics"] = "surgewave-topic1,surgewave-topic2",
            ["azure.servicebus.queue.name"] = "my-queue"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    private static ConnectorContext CreateContext()
    {
        return new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { },
            Logger = null
        };
    }
}
