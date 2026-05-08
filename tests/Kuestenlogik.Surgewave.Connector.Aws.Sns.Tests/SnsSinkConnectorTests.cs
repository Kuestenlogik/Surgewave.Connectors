namespace Kuestenlogik.Surgewave.Connector.Aws.Sns.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class SnsSinkConnectorTests
{
    [Fact]
    public void SnsSinkConnector_HasCorrectVersion()
    {
        using var connector = new SnsSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void SnsSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new SnsSinkConnector();
        Assert.Equal(typeof(SnsSinkTask), connector.TaskClass);
    }

    [Fact]
    public void SnsSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new SnsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "aws.sns.topic.arn" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "topics" && k.Type == ConfigType.String);
    }

    [Fact]
    public void SnsSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new SnsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "aws.region");
        Assert.Contains(config.Keys, k => k.Name == "aws.access.key");
        Assert.Contains(config.Keys, k => k.Name == "aws.secret.key");
        Assert.Contains(config.Keys, k => k.Name == "aws.endpoint");
        Assert.Contains(config.Keys, k => k.Name == "aws.sns.subject");
        Assert.Contains(config.Keys, k => k.Name == "aws.sns.message.group.id");
        Assert.Contains(config.Keys, k => k.Name == "aws.sns.header.prefix");
    }

    [Fact]
    public void SnsSinkConnector_Start_ThrowsOnMissingTopicArn()
    {
        using var connector = new SnsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topics"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("aws.sns.topic.arn", ex.Message);
    }

    [Fact]
    public void SnsSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new SnsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sns.topic.arn"] = "arn:aws:sns:us-east-1:123456789:my-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void SnsSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new SnsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sns.topic.arn"] = "arn:aws:sns:us-east-1:123456789:my-topic",
            ["topics"] = "surgewave-topic1,surgewave-topic2"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    [Fact]
    public void SnsSinkConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new SnsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["aws.sns.topic.arn"] = "arn:aws:sns:us-east-1:123456789:my-topic",
            ["topics"] = "surgewave-topic",
            ["aws.region"] = "eu-west-1",
            ["aws.sns.subject"] = "Test Subject"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("arn:aws:sns:us-east-1:123456789:my-topic", taskConfigs[0]["aws.sns.topic.arn"]);
        Assert.Equal("surgewave-topic", taskConfigs[0]["topics"]);
        Assert.Equal("eu-west-1", taskConfigs[0]["aws.region"]);
        Assert.Equal("Test Subject", taskConfigs[0]["aws.sns.subject"]);
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
