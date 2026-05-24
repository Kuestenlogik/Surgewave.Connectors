namespace Kuestenlogik.Surgewave.Connector.Azure.ServiceBus.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class ServiceBusSourceConnectorTests
{
    [Fact]
    public void ServiceBusSourceConnector_HasCorrectVersion()
    {
        using var connector = new ServiceBusSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void ServiceBusSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new ServiceBusSourceConnector();
        Assert.Equal(typeof(ServiceBusSourceTask), connector.TaskClass);
    }

    [Fact]
    public void ServiceBusSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new ServiceBusSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.connection.string" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "surgewave.topic" && k.Type == ConfigType.String);
    }

    [Fact]
    public void ServiceBusSourceConnector_Config_HasOptionalKeys()
    {
        using var connector = new ServiceBusSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.queue.name");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.topic.name");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.subscription.name");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.receive.mode");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.prefetch.count");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.max.messages");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.header.prefix");
        Assert.Contains(config.Keys, k => k.Name == "azure.servicebus.include.metadata");
    }

    [Fact]
    public void ServiceBusSourceConnector_Start_ThrowsOnMissingConnectionString()
    {
        using var connector = new ServiceBusSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["surgewave.topic"] = "surgewave-topic",
            ["azure.servicebus.queue.name"] = "my-queue"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("azure.servicebus.connection.string", ex.Message);
    }

    [Fact]
    public void ServiceBusSourceConnector_Start_ThrowsOnMissingSurgewaveTopic()
    {
        using var connector = new ServiceBusSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["azure.servicebus.queue.name"] = "my-queue"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("surgewave.topic", ex.Message);
    }

    [Fact]
    public void ServiceBusSourceConnector_Start_ThrowsOnMissingQueueAndTopic()
    {
        using var connector = new ServiceBusSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["surgewave.topic"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Must specify", ex.Message);
    }

    [Fact]
    public void ServiceBusSourceConnector_Start_ThrowsOnTopicWithoutSubscription()
    {
        using var connector = new ServiceBusSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["surgewave.topic"] = "surgewave-topic",
            ["azure.servicebus.topic.name"] = "my-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("subscription", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ServiceBusSourceConnector_Start_ThrowsOnBothQueueAndTopic()
    {
        using var connector = new ServiceBusSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["surgewave.topic"] = "surgewave-topic",
            ["azure.servicebus.queue.name"] = "my-queue",
            ["azure.servicebus.topic.name"] = "my-topic",
            ["azure.servicebus.subscription.name"] = "my-subscription"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mutually exclusive", ex.Message);
    }

    [Fact]
    public void ServiceBusSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new ServiceBusSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["surgewave.topic"] = "surgewave-topic",
            ["azure.servicebus.queue.name"] = "my-queue"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    [Fact]
    public void ServiceBusSourceConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new ServiceBusSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["azure.servicebus.connection.string"] = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            ["surgewave.topic"] = "surgewave-topic",
            ["azure.servicebus.queue.name"] = "my-queue",
            ["azure.servicebus.receive.mode"] = "ReceiveAndDelete"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test",
            taskConfigs[0]["azure.servicebus.connection.string"]);
        Assert.Equal("surgewave-topic", taskConfigs[0]["surgewave.topic"]);
        Assert.Equal("my-queue", taskConfigs[0]["azure.servicebus.queue.name"]);
        Assert.Equal("ReceiveAndDelete", taskConfigs[0]["azure.servicebus.receive.mode"]);
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
