namespace Kuestenlogik.Surgewave.Connector.Gcp.PubSub.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class PubSubSinkConnectorTests
{
    [Fact]
    public void PubSubSinkConnector_HasCorrectVersion()
    {
        using var connector = new PubSubSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void PubSubSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new PubSubSinkConnector();
        Assert.Equal(typeof(PubSubSinkTask), connector.TaskClass);
    }

    [Fact]
    public void PubSubSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new PubSubSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.project.id" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.topic.id" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "topics" && k.Type == ConfigType.String);
    }

    [Fact]
    public void PubSubSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new PubSubSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.credentials.json");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.credentials.file");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.emulator.host");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.ordering.key.field");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.batch.size");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.batch.delay.ms");
        Assert.Contains(config.Keys, k => k.Name == "gcp.pubsub.header.prefix");
    }

    [Fact]
    public void PubSubSinkConnector_Start_ThrowsOnMissingProjectId()
    {
        using var connector = new PubSubSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.topic.id"] = "my-topic",
            ["topics"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("gcp.pubsub.project.id", ex.Message);
    }

    [Fact]
    public void PubSubSinkConnector_Start_ThrowsOnMissingTopicId()
    {
        using var connector = new PubSubSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["topics"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("gcp.pubsub.topic.id", ex.Message);
    }

    [Fact]
    public void PubSubSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new PubSubSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["gcp.pubsub.topic.id"] = "my-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1001)]
    [InlineData(-5)]
    public void PubSubSinkConnector_Start_ThrowsOnInvalidBatchSize(int batchSize)
    {
        using var connector = new PubSubSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["gcp.pubsub.topic.id"] = "my-topic",
            ["topics"] = "surgewave-topic",
            ["gcp.pubsub.batch.size"] = batchSize.ToString()
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("batch size", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PubSubSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new PubSubSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["gcp.pubsub.project.id"] = "my-project",
            ["gcp.pubsub.topic.id"] = "my-topic",
            ["topics"] = "surgewave-topic1,surgewave-topic2"
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
