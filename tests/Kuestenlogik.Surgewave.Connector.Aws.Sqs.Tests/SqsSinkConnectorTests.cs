namespace Kuestenlogik.Surgewave.Connector.Aws.Sqs.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class SqsSinkConnectorTests
{
    [Fact]
    public void SqsSinkConnector_HasCorrectVersion()
    {
        using var connector = new SqsSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void SqsSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new SqsSinkConnector();
        Assert.Equal(typeof(SqsSinkTask), connector.TaskClass);
    }

    [Fact]
    public void SqsSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new SqsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.queue.url" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "topics" && k.Type == ConfigType.String);
    }

    [Fact]
    public void SqsSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new SqsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "aws.region");
        Assert.Contains(config.Keys, k => k.Name == "aws.access.key");
        Assert.Contains(config.Keys, k => k.Name == "aws.secret.key");
        Assert.Contains(config.Keys, k => k.Name == "aws.endpoint");
        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.message.group.id.field");
        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.deduplication.id.field");
        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.header.prefix");
    }

    [Fact]
    public void SqsSinkConnector_Start_ThrowsOnMissingQueueUrl()
    {
        using var connector = new SqsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topics"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("aws.sqs.queue.url", ex.Message);
    }

    [Fact]
    public void SqsSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new SqsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sqs.queue.url"] = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void SqsSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new SqsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sqs.queue.url"] = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
            ["topics"] = "surgewave-topic1,surgewave-topic2"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    [Fact]
    public void SqsSinkConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new SqsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sqs.queue.url"] = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
            ["topics"] = "surgewave-topic",
            ["aws.region"] = "eu-west-1",
            ["aws.sqs.message.group.id.field"] = "key"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("https://sqs.us-east-1.amazonaws.com/123456789/my-queue", taskConfigs[0]["aws.sqs.queue.url"]);
        Assert.Equal("surgewave-topic", taskConfigs[0]["topics"]);
        Assert.Equal("eu-west-1", taskConfigs[0]["aws.region"]);
        Assert.Equal("key", taskConfigs[0]["aws.sqs.message.group.id.field"]);
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
