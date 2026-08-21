namespace Kuestenlogik.Surgewave.Connector.Beanstalkd.Tests;

/// <summary>
/// Configuration validation for the beanstalkd source and sink connectors.
/// </summary>
public class BeanstalkdConnectorTests
{
    [Fact]
    public void SourceConnector_StartRejectsAMissingTopic()
    {
        using var connector = new BeanstalkdSourceConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [BeanstalkdConnectorConfig.Tube] = "inbox" }));

        Assert.Contains(BeanstalkdConnectorConfig.Topic, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_StartRejectsAMissingTube()
    {
        using var connector = new BeanstalkdSourceConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [BeanstalkdConnectorConfig.Topic] = "jobs" }));

        Assert.Contains(BeanstalkdConnectorConfig.Tube, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_StartRejectsMissingTopics()
    {
        using var connector = new BeanstalkdSinkConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [BeanstalkdConnectorConfig.Tube] = "inbox" }));

        Assert.Contains(BeanstalkdConnectorConfig.Topics, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SinkConnector_StartRejectsAMissingTube()
    {
        using var connector = new BeanstalkdSinkConnector();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(
            new Dictionary<string, string> { [BeanstalkdConnectorConfig.Topics] = "jobs" }));

        Assert.Contains(BeanstalkdConnectorConfig.Tube, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SourceConnector_HandsTheWholeConfigurationToASingleTask()
    {
        using var connector = new BeanstalkdSourceConnector();
        var config = new Dictionary<string, string>
        {
            [BeanstalkdConnectorConfig.Topic] = "jobs",
            [BeanstalkdConnectorConfig.Tube] = "inbox",
            [BeanstalkdConnectorConfig.Host] = "beans.example.com"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        // A single beanstalkd connection cannot be sharded across tasks.
        var taskConfig = Assert.Single(taskConfigs);
        Assert.Equal("beans.example.com", taskConfig[BeanstalkdConnectorConfig.Host]);
        Assert.Equal(typeof(BeanstalkdSourceTask), connector.TaskClass);
    }
}
