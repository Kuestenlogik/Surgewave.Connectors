namespace Kuestenlogik.Surgewave.Connector.Slack.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class SlackSinkConnectorTests
{
    [Fact]
    public void SlackSinkConnector_HasCorrectVersion()
    {
        using var connector = new SlackSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void SlackSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new SlackSinkConnector();
        Assert.Equal(typeof(SlackSinkTask), connector.TaskClass);
    }

    [Fact]
    public void SlackSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new SlackSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.BotTokenConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.DefaultChannelConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void SlackSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new SlackSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.ChannelFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.TextFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.TextTemplateConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.BlocksFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.AttachmentsFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.ThreadTsFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.UsernameConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.IconEmojiConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.IconUrlConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.MarkdownConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.UnfurlLinksConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.UnfurlMediaConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.AddReactionConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.ReactionFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.DefaultReactionConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.RetryCountConfig);
        Assert.Contains(config.Keys, k => k.Name == SlackConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void SlackSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new SlackSinkConnector();
        var config = connector.Config;

        var textTemplateKey = config.Keys.First(k => k.Name == SlackConnectorConfig.TextTemplateConfig);
        Assert.Equal(SlackConnectorConfig.DefaultTextTemplate, textTemplateKey.DefaultValue);

        var markdownKey = config.Keys.First(k => k.Name == SlackConnectorConfig.MarkdownConfig);
        Assert.Equal(SlackConnectorConfig.DefaultMarkdown, markdownKey.DefaultValue);

        var unfurlLinksKey = config.Keys.First(k => k.Name == SlackConnectorConfig.UnfurlLinksConfig);
        Assert.Equal(SlackConnectorConfig.DefaultUnfurlLinks, unfurlLinksKey.DefaultValue);

        var unfurlMediaKey = config.Keys.First(k => k.Name == SlackConnectorConfig.UnfurlMediaConfig);
        Assert.Equal(SlackConnectorConfig.DefaultUnfurlMedia, unfurlMediaKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == SlackConnectorConfig.BatchSizeConfig);
        Assert.Equal(SlackConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var retryCountKey = config.Keys.First(k => k.Name == SlackConnectorConfig.RetryCountConfig);
        Assert.Equal(SlackConnectorConfig.DefaultRetryCount, retryCountKey.DefaultValue);

        var retryDelayKey = config.Keys.First(k => k.Name == SlackConnectorConfig.RetryDelayMsConfig);
        Assert.Equal(SlackConnectorConfig.DefaultRetryDelayMs, retryDelayKey.DefaultValue);
    }

    [Fact]
    public void SlackSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new SlackSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#general"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SlackConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void SlackSinkConnector_Start_ThrowsOnMissingBotToken()
    {
        using var connector = new SlackSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.DefaultChannelConfig] = "#general"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SlackConnectorConfig.BotTokenConfig, ex.Message);
    }

    [Fact]
    public void SlackSinkConnector_Start_ThrowsOnMissingChannelConfig()
    {
        using var connector = new SlackSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("channel", ex.Message.ToLowerInvariant());
    }

    [Fact]
    public void SlackSinkConnector_Start_SucceedsWithDefaultChannel()
    {
        using var connector = new SlackSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#general"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void SlackSinkConnector_Start_SucceedsWithChannelField()
    {
        using var connector = new SlackSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.ChannelFieldConfig] = "channel"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void SlackSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new SlackSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#general"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        Assert.Single(taskConfigs);
    }

    [Fact]
    public void SlackSinkConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new SlackSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts,notifications",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.UsernameConfig] = "SurgewaveBot",
            [SlackConnectorConfig.IconEmojiConfig] = ":robot_face:"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("alerts,notifications", taskConfigs[0][SlackConnectorConfig.TopicsConfig]);
        Assert.Equal("xoxb-test-token", taskConfigs[0][SlackConnectorConfig.BotTokenConfig]);
        Assert.Equal("#alerts", taskConfigs[0][SlackConnectorConfig.DefaultChannelConfig]);
        Assert.Equal("SurgewaveBot", taskConfigs[0][SlackConnectorConfig.UsernameConfig]);
        Assert.Equal(":robot_face:", taskConfigs[0][SlackConnectorConfig.IconEmojiConfig]);
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
