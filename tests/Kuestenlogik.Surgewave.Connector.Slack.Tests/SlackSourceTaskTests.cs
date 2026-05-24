namespace Kuestenlogik.Surgewave.Connector.Slack.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class SlackSourceTaskTests
{
    [Fact]
    public void SlackSourceTask_HasCorrectVersion()
    {
        using var task = new SlackSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void SlackSourceTask_Start_InitializesWithValidConfig()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token"
        };

        // Start should succeed (actual connection happens in PollAsync)
        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSourceTask_Start_ParsesEventTypes()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token",
            [SlackConnectorConfig.EventTypesConfig] = "message,reaction_added,app_mention"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSourceTask_Start_ParsesChannelFilter()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token",
            [SlackConnectorConfig.ChannelFilterConfig] = "C123456,C789012"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSourceTask_Start_ParsesUserFilter()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token",
            [SlackConnectorConfig.UserFilterConfig] = "U123456,U789012"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSourceTask_Start_ParsesIncludeBotMessages()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token",
            [SlackConnectorConfig.IncludeBotMessagesConfig] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSourceTask_Stop_CanBeCalledAfterStart()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token"
        };

        task.Start(config);
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSourceTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token"
        };

        task.Start(config);
        task.Stop();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public async Task SlackSourceTask_CommitAsync_CompletesSuccessfully()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token"
        };

        task.Start(config);
        await task.CommitAsync(CancellationToken.None);
    }

    [Fact]
    public void SlackSourceTask_CurrentOffset_InitiallyNull()
    {
        using var task = new SlackSourceTask();
        Assert.Null(task.CurrentOffset);
    }

    [Fact]
    public void SlackSourceTask_Start_WithAllEventTypes()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token",
            [SlackConnectorConfig.EventTypesConfig] = "message,reaction_added,reaction_removed,app_mention,channel_created,channel_archive,member_joined_channel,member_left_channel,file_shared"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSourceTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token"
        };

        task.Start(config);
        task.Dispose();
        var exception = Record.Exception(() => task.Dispose());
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSourceTask_Start_WithEmptyEventTypesUsesDefaults()
    {
        using var task = new SlackSourceTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicConfig] = "slack-events",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.AppTokenConfig] = "xapp-test-token",
            [SlackConnectorConfig.EventTypesConfig] = "" // Empty should use defaults
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }
}
