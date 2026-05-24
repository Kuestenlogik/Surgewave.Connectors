namespace Kuestenlogik.Surgewave.Connector.Slack.Tests;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class SlackSinkTaskTests
{
    [Fact]
    public void SlackSinkTask_HasCorrectVersion()
    {
        using var task = new SlackSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void SlackSinkTask_Start_InitializesWithValidConfig()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#general"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_ParsesTextTemplate()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.TextTemplateConfig] = "Alert: ${value} from ${key}"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_ParsesChannelField()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.ChannelFieldConfig] = "target_channel"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_ParsesTextField()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.TextFieldConfig] = "message"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_ParsesBlocksAndAttachmentsFields()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.BlocksFieldConfig] = "blocks",
            [SlackConnectorConfig.AttachmentsFieldConfig] = "attachments"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_ParsesThreadTsField()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.ThreadTsFieldConfig] = "parent_ts"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_ParsesAppearanceSettings()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.UsernameConfig] = "AlertBot",
            [SlackConnectorConfig.IconEmojiConfig] = ":warning:",
            [SlackConnectorConfig.IconUrlConfig] = "https://example.com/icon.png"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_ParsesUnfurlSettings()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.UnfurlLinksConfig] = "false",
            [SlackConnectorConfig.UnfurlMediaConfig] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_ParsesReactionSettings()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.AddReactionConfig] = "true",
            [SlackConnectorConfig.ReactionFieldConfig] = "emoji",
            [SlackConnectorConfig.DefaultReactionConfig] = "thumbsup"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_ParsesRetrySettings()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.RetryCountConfig] = "5",
            [SlackConnectorConfig.RetryDelayMsConfig] = "2000"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Stop_CleansUpResources()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts"
        };

        task.Start(config);
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts"
        };

        task.Start(config);
        task.Stop();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public async Task SlackSinkTask_FlushAsync_CompletesSuccessfully()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts"
        };

        task.Start(config);
        var offsets = new Dictionary<TopicPartition, long>
        {
            [new TopicPartition("alerts", 0)] = 100
        };

        await task.FlushAsync(offsets, CancellationToken.None);
    }

    [Fact]
    public async Task SlackSinkTask_PutAsync_ThrowsWhenNotStarted()
    {
        using var task = new SlackSinkTask();
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "alerts",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("key1"),
                Value = Encoding.UTF8.GetBytes("{\"message\": \"test\"}")
            }
        };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync(records, CancellationToken.None));
    }

    [Fact]
    public void SlackSinkTask_Start_WithAllSettings()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts,notifications",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.ChannelFieldConfig] = "channel",
            [SlackConnectorConfig.TextFieldConfig] = "text",
            [SlackConnectorConfig.TextTemplateConfig] = "Message: ${value}",
            [SlackConnectorConfig.BlocksFieldConfig] = "blocks",
            [SlackConnectorConfig.AttachmentsFieldConfig] = "attachments",
            [SlackConnectorConfig.ThreadTsFieldConfig] = "thread_ts",
            [SlackConnectorConfig.UsernameConfig] = "SurgewaveBot",
            [SlackConnectorConfig.IconEmojiConfig] = ":robot_face:",
            [SlackConnectorConfig.UnfurlLinksConfig] = "true",
            [SlackConnectorConfig.UnfurlMediaConfig] = "true",
            [SlackConnectorConfig.AddReactionConfig] = "false",
            [SlackConnectorConfig.RetryCountConfig] = "3",
            [SlackConnectorConfig.RetryDelayMsConfig] = "1000"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_WithInvalidRetryCount_UsesDefault()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.RetryCountConfig] = "invalid"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void SlackSinkTask_Start_WithInvalidRetryDelay_UsesDefault()
    {
        using var task = new SlackSinkTask();
        var config = new Dictionary<string, string>
        {
            [SlackConnectorConfig.TopicsConfig] = "alerts",
            [SlackConnectorConfig.BotTokenConfig] = "xoxb-test-token",
            [SlackConnectorConfig.DefaultChannelConfig] = "#alerts",
            [SlackConnectorConfig.RetryDelayMsConfig] = "invalid"
        };

        var exception = Record.Exception(() => task.Start(config));
        Assert.Null(exception);
    }
}
