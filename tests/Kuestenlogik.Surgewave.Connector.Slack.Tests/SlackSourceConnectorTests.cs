namespace Kuestenlogik.Surgewave.Connector.Slack.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class SlackSourceConnectorTests
{
    [Fact]
    public void SlackSourceConnector_HasCorrectVersion()
    {
        using var connector = new SlackSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void SlackSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new SlackSourceConnector();
        Assert.Equal(typeof(SlackSourceTask), connector.TaskClass);
    }

    [Fact]
    public void SlackSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new SlackSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.TopicConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.BotTokenConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.AppTokenConfig && k.Type == ConfigType.Password);
    }

    [Fact]
    public void SlackSourceConnector_Config_HasOptionalKeys()
    {
        using var connector = new SlackSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.SigningSecretConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.SocketModeConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.EventTypesConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.ChannelFilterConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.UserFilterConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.IncludeBotMessagesConfig);
    }

    [Fact]
    public void SlackSourceConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new SlackSourceConnector();
        var config = connector.Config;

        var socketModeKey = config.Keys.First(k => k.Name == SlackConnectorConfig.SocketModeConfig);
        Assert.Equal(SlackConnectorConfig.DefaultSocketMode, socketModeKey.DefaultValue);

        var eventTypesKey = config.Keys.First(k => k.Name == SlackConnectorConfig.EventTypesConfig);
        Assert.Equal(SlackConnectorConfig.DefaultEventTypes, eventTypesKey.DefaultValue);

        var includeBotMessagesKey = config.Keys.First(k => k.Name == SlackConnectorConfig.IncludeBotMessagesConfig);
        Assert.Equal(SlackConnectorConfig.DefaultIncludeBotMessages, includeBotMessagesKey.DefaultValue);
    }

    [Fact]
    public void SlackSourceConnector_Start_ThrowsOnMissingTopic()
    {
        using var connector = new SlackSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SlackConnectorConfig.TopicConfig, ex.Message);
    }

    [Fact]
    public void SlackSourceConnector_Start_ThrowsOnMissingBotToken()
    {
        using var connector = new SlackSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SlackConnectorConfig.BotTokenConfig, ex.Message);
    }

    [Fact]
    public void SlackSourceConnector_Start_ThrowsOnMissingAppTokenForSocketMode()
    {
        using var connector = new SlackSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.SocketModeConfig] = "true"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SlackConnectorConfig.AppTokenConfig, ex.Message);
    }

    [Fact]
    public void SlackSourceConnector_Start_SucceedsWithValidConfig()
    {
        using var connector = new SlackSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void SlackSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new SlackSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    [Fact]
    public void SlackSourceConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new SlackSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token",
            [SlackConnectorConfig.EventTypesConfig] = "message,reaction_added",
            [SlackConnectorConfig.ChannelFilterConfig] = "C123456,C789012"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("slack-events", taskConfigs[0][SlackConnectorConfig.TopicConfig]);
        Assert.Equal("xoxb-test-token", taskConfigs[0][SlackConnectorConfig.BotTokenConfig]);
        Assert.Equal("xapp-test-token", taskConfigs[0][SlackConnectorConfig.AppTokenConfig]);
        Assert.Equal("message,reaction_added", taskConfigs[0][SlackConnectorConfig.EventTypesConfig]);
        Assert.Equal("C123456,C789012", taskConfigs[0][SlackConnectorConfig.ChannelFilterConfig]);
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
