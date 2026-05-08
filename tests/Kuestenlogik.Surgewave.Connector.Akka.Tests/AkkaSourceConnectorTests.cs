using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka.Tests;

public class AkkaSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        var connector = new AkkaSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSourceTaskType()
    {
        var connector = new AkkaSourceConnector();
        Assert.Equal(typeof(AkkaSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsActorSystemNameConfig()
    {
        var connector = new AkkaSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.ActorSystemNameConfig);
    }

    [Fact]
    public void Config_ContainsActorSystemConfigConfig()
    {
        var connector = new AkkaSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.ActorSystemConfigConfig);
    }

    [Fact]
    public void Config_ContainsActorPathConfig()
    {
        var connector = new AkkaSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.ActorPathConfig);
    }

    [Fact]
    public void Config_ContainsTopicPatternConfig()
    {
        var connector = new AkkaSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_ContainsPollTimeoutMsConfig()
    {
        var connector = new AkkaSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.PollTimeoutMsConfig);
    }

    [Fact]
    public void Config_ContainsMaxMessagesPerPollConfig()
    {
        var connector = new AkkaSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.MaxMessagesPerPollConfig);
    }

    [Fact]
    public void Config_ContainsIncludeMetadataConfig()
    {
        var connector = new AkkaSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == AkkaConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_ActorSystemNameHasDefaultValue()
    {
        var connector = new AkkaSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == AkkaConnectorConfig.ActorSystemNameConfig);
        Assert.Equal(AkkaConnectorConfig.DefaultActorSystemName, key.DefaultValue);
    }

    [Fact]
    public void Start_Succeeds()
    {
        var connector = new AkkaSourceConnector();
        var config = new Dictionary<string, string>();

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new AkkaSourceConnector();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/test"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        connector.Stop();
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new AkkaSourceConnector();
        connector.Start(new Dictionary<string, string>());
        connector.Stop();
        connector.Stop();
    }
}
