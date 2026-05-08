using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka.Tests;

public class AkkaSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        var connector = new AkkaSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSinkTaskType()
    {
        var connector = new AkkaSinkConnector();
        Assert.Equal(typeof(AkkaSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsActorSystemNameConfig()
    {
        var connector = new AkkaSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.ActorSystemNameConfig);
    }

    [Fact]
    public void Config_ContainsActorPathConfig()
    {
        var connector = new AkkaSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.ActorPathConfig);
    }

    [Fact]
    public void Config_ContainsTopicsConfig()
    {
        var connector = new AkkaSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_ContainsAskTimeoutMsConfig()
    {
        var connector = new AkkaSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.AskTimeoutMsConfig);
    }

    [Fact]
    public void Config_ContainsTellOnlyConfig()
    {
        var connector = new AkkaSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.TellOnlyConfig);
    }

    [Fact]
    public void Config_ContainsBatchSizeConfig()
    {
        var connector = new AkkaSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_ContainsMaxRetryCountConfig()
    {
        var connector = new AkkaSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void Config_ActorPathIsHighImportance()
    {
        var connector = new AkkaSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == AkkaConnectorConfig.ActorPathConfig);
        Assert.Equal(Importance.High, key.Importance);
    }

    [Fact]
    public void Config_TopicsIsHighImportance()
    {
        var connector = new AkkaSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == AkkaConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, key.Importance);
    }

    [Fact]
    public void Start_WithActorPathAndTopics_Succeeds()
    {
        var connector = new AkkaSinkConnector();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor",
            [AkkaConnectorConfig.TopicsConfig] = "topic1,topic2"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithoutActorPath_ThrowsArgumentException()
    {
        var connector = new AkkaSinkConnector();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.TopicsConfig] = "topic1"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithoutTopics_ThrowsArgumentException()
    {
        var connector = new AkkaSinkConnector();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new AkkaSinkConnector();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor",
            [AkkaConnectorConfig.TopicsConfig] = "topic1"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        connector.Stop();
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new AkkaSinkConnector();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor",
            [AkkaConnectorConfig.TopicsConfig] = "topic1"
        };

        connector.Start(config);
        connector.Stop();
        connector.Stop();
    }
}
