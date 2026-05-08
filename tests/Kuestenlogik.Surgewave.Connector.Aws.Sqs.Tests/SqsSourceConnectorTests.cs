namespace Kuestenlogik.Surgewave.Connector.Aws.Sqs.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class SqsSourceConnectorTests
{
    [Fact]
    public void SqsSourceConnector_HasCorrectVersion()
    {
        using var connector = new SqsSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void SqsSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new SqsSourceConnector();
        Assert.Equal(typeof(SqsSourceTask), connector.TaskClass);
    }

    [Fact]
    public void SqsSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new SqsSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.queue.url" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "surgewave.topic" && k.Type == ConfigType.String);
    }

    [Fact]
    public void SqsSourceConnector_Config_HasOptionalKeys()
    {
        using var connector = new SqsSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "aws.region");
        Assert.Contains(config.Keys, k => k.Name == "aws.access.key");
        Assert.Contains(config.Keys, k => k.Name == "aws.secret.key");
        Assert.Contains(config.Keys, k => k.Name == "aws.endpoint");
        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.wait.time.seconds");
        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.visibility.timeout");
        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.max.messages");
        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.header.prefix");
        Assert.Contains(config.Keys, k => k.Name == "aws.sqs.include.metadata");
    }

    [Fact]
    public void SqsSourceConnector_Start_ThrowsOnMissingQueueUrl()
    {
        using var connector = new SqsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["surgewave.topic"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("aws.sqs.queue.url", ex.Message);
    }

    [Fact]
    public void SqsSourceConnector_Start_ThrowsOnMissingSurgewaveTopic()
    {
        using var connector = new SqsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sqs.queue.url"] = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("surgewave.topic", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(11)]
    [InlineData(-1)]
    public void SqsSourceConnector_Start_ThrowsOnInvalidMaxMessages(int maxMessages)
    {
        using var connector = new SqsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sqs.queue.url"] = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
            ["surgewave.topic"] = "surgewave-topic",
            ["aws.sqs.max.messages"] = maxMessages.ToString()
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("max messages", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(21)]
    public void SqsSourceConnector_Start_ThrowsOnInvalidWaitTime(int waitTime)
    {
        using var connector = new SqsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sqs.queue.url"] = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
            ["surgewave.topic"] = "surgewave-topic",
            ["aws.sqs.wait.time.seconds"] = waitTime.ToString()
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("wait time", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(10)]
    public void SqsSourceConnector_Start_AcceptsValidMaxMessages(int maxMessages)
    {
        using var connector = new SqsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sqs.queue.url"] = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
            ["surgewave.topic"] = "surgewave-topic",
            ["aws.sqs.max.messages"] = maxMessages.ToString()
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void SqsSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new SqsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sqs.queue.url"] = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
            ["surgewave.topic"] = "surgewave-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    [Fact]
    public void SqsSourceConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new SqsSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sqs.queue.url"] = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
            ["surgewave.topic"] = "surgewave-topic",
            ["aws.region"] = "eu-west-1",
            ["aws.sqs.max.messages"] = "5"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("https://sqs.us-east-1.amazonaws.com/123456789/my-queue", taskConfigs[0]["aws.sqs.queue.url"]);
        Assert.Equal("surgewave-topic", taskConfigs[0]["surgewave.topic"]);
        Assert.Equal("eu-west-1", taskConfigs[0]["aws.region"]);
        Assert.Equal("5", taskConfigs[0]["aws.sqs.max.messages"]);
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
