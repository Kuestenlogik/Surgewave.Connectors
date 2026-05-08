namespace Kuestenlogik.Surgewave.Connector.Gcp.PubSub.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class PubSubSourceConnectorTests
{
    [Fact]
    public void PubSubSourceConnector_HasCorrectVersion()
    {
        using var connector = new PubSubSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void PubSubSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new PubSubSourceConnector();
        Assert.Equal(typeof(PubSubSourceTask), connector.TaskClass);
    }

    [Fact]
    public void PubSubSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new PubSubSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.project.id" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.subscription.id" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "surgewave.topic" && k.Type == ConfigType.String);
    }

    [Fact]
    public void PubSubSourceConnector_Config_HasOptionalKeys()
    {
        using var connector = new PubSubSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.credentials.json");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.credentials.file");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.emulator.host");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.max.messages");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.ack.deadline.seconds");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.auto.ack");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.header.prefix");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.include.metadata");
    }

    [Fact]
    public void PubSubSourceConnector_Start_ThrowsOnMissingProjectId()
    {
        using var connector = new PubSubSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.subscription.id"] = "my-subscription",
            ["surgewave.topic"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("gcp.pubsub.project.id", ex.Message);
    }

    [Fact]
    public void PubSubSourceConnector_Start_ThrowsOnMissingSubscriptionId()
    {
        using var connector = new PubSubSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["surgewave.topic"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("gcp.pubsub.subscription.id", ex.Message);
    }

    [Fact]
    public void PubSubSourceConnector_Start_ThrowsOnMissingSurgewaveTopic()
    {
        using var connector = new PubSubSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["gcp.pubsub.subscription.id"] = "my-subscription"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("surgewave.topic", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1001)]
    [InlineData(-5)]
    public void PubSubSourceConnector_Start_ThrowsOnInvalidMaxMessages(int maxMessages)
    {
        using var connector = new PubSubSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["gcp.pubsub.subscription.id"] = "my-subscription",
            ["surgewave.topic"] = "surgewave-topic",
            ["gcp.pubsub.max.messages"] = maxMessages.ToString()
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("max messages", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1000)]
    public void PubSubSourceConnector_Start_AcceptsValidMaxMessages(int maxMessages)
    {
        using var connector = new PubSubSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["gcp.pubsub.subscription.id"] = "my-subscription",
            ["surgewave.topic"] = "surgewave-topic",
            ["gcp.pubsub.max.messages"] = maxMessages.ToString()
        };

        // Should not throw during validation
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void PubSubSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new PubSubSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["gcp.pubsub.subscription.id"] = "my-subscription",
            ["surgewave.topic"] = "surgewave-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        // Pub/Sub subscriptions are shared, so only one task
        Assert.Single(taskConfigs);
    }

    [Fact]
    public void PubSubSourceConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new PubSubSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["gcp.pubsub.subscription.id"] = "my-subscription",
            ["surgewave.topic"] = "surgewave-topic",
            ["gcp.pubsub.max.messages"] = "50",
            ["gcp.pubsub.auto.ack"] = "false"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("my-project", taskConfigs[0]["gcp.pubsub.project.id"]);
        Assert.Equal("my-subscription", taskConfigs[0]["gcp.pubsub.subscription.id"]);
        Assert.Equal("surgewave-topic", taskConfigs[0]["surgewave.topic"]);
        Assert.Equal("50", taskConfigs[0]["gcp.pubsub.max.messages"]);
        Assert.Equal("false", taskConfigs[0]["gcp.pubsub.auto.ack"]);
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
