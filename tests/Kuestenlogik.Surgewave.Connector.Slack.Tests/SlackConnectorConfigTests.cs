namespace Kuestenlogik.Surgewave.Connector.Slack.Tests;

using Xunit;

public sealed class SlackConnectorConfigTests
{
    [Fact]
    public void SlackConnectorConfig_TopicConfigKeys_AreCorrect()
    {
        Assert.Equal("topic", SlackConnectorConfig.TopicConfig);
        Assert.Equal("topics", SlackConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void SlackConnectorConfig_ApiConfigKeys_AreCorrect()
    {
        Assert.Equal("slack.bot.token", SlackConnectorConfig.BotTokenConfig);
        Assert.Equal("slack.app.token", SlackConnectorConfig.AppTokenConfig);
        Assert.Equal("slack.signing.secret", SlackConnectorConfig.SigningSecretConfig);
    }

    [Fact]
    public void SlackConnectorConfig_SourceConfigKeys_AreCorrect()
    {
        Assert.Equal("slack.socket.mode", SlackConnectorConfig.SocketModeConfig);
        Assert.Equal("slack.event.types", SlackConnectorConfig.EventTypesConfig);
        Assert.Equal("slack.channel.filter", SlackConnectorConfig.ChannelFilterConfig);
        Assert.Equal("slack.user.filter", SlackConnectorConfig.UserFilterConfig);
        Assert.Equal("slack.include.bot.messages", SlackConnectorConfig.IncludeBotMessagesConfig);
    }

    [Fact]
    public void SlackConnectorConfig_SinkConfigKeys_AreCorrect()
    {
        Assert.Equal("slack.default.channel", SlackConnectorConfig.DefaultChannelConfig);
        Assert.Equal("slack.channel.field", SlackConnectorConfig.ChannelFieldConfig);
        Assert.Equal("slack.text.field", SlackConnectorConfig.TextFieldConfig);
        Assert.Equal("slack.blocks.field", SlackConnectorConfig.BlocksFieldConfig);
        Assert.Equal("slack.attachments.field", SlackConnectorConfig.AttachmentsFieldConfig);
        Assert.Equal("slack.thread.ts.field", SlackConnectorConfig.ThreadTsFieldConfig);
        Assert.Equal("slack.username", SlackConnectorConfig.UsernameConfig);
        Assert.Equal("slack.icon.emoji", SlackConnectorConfig.IconEmojiConfig);
        Assert.Equal("slack.icon.url", SlackConnectorConfig.IconUrlConfig);
        Assert.Equal("slack.unfurl.links", SlackConnectorConfig.UnfurlLinksConfig);
        Assert.Equal("slack.unfurl.media", SlackConnectorConfig.UnfurlMediaConfig);
    }

    [Fact]
    public void SlackConnectorConfig_MessageFormattingKeys_AreCorrect()
    {
        Assert.Equal("slack.text.template", SlackConnectorConfig.TextTemplateConfig);
        Assert.Equal("slack.markdown", SlackConnectorConfig.MarkdownConfig);
    }

    [Fact]
    public void SlackConnectorConfig_ReactionKeys_AreCorrect()
    {
        Assert.Equal("slack.add.reaction", SlackConnectorConfig.AddReactionConfig);
        Assert.Equal("slack.reaction.field", SlackConnectorConfig.ReactionFieldConfig);
        Assert.Equal("slack.default.reaction", SlackConnectorConfig.DefaultReactionConfig);
    }

    [Fact]
    public void SlackConnectorConfig_FileKeys_AreCorrect()
    {
        Assert.Equal("slack.file.field", SlackConnectorConfig.FileFieldConfig);
        Assert.Equal("slack.filename.field", SlackConnectorConfig.FileNameFieldConfig);
        Assert.Equal("slack.file.title.field", SlackConnectorConfig.FileTitleFieldConfig);
    }

    [Fact]
    public void SlackConnectorConfig_BehaviorKeys_AreCorrect()
    {
        Assert.Equal("slack.batch.size", SlackConnectorConfig.BatchSizeConfig);
        Assert.Equal("slack.retry.count", SlackConnectorConfig.RetryCountConfig);
        Assert.Equal("slack.retry.delay.ms", SlackConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void SlackConnectorConfig_DefaultValues_AreCorrect()
    {
        Assert.True(SlackConnectorConfig.DefaultSocketMode);
        Assert.False(SlackConnectorConfig.DefaultIncludeBotMessages);
        Assert.Equal("${value}", SlackConnectorConfig.DefaultTextTemplate);
        Assert.True(SlackConnectorConfig.DefaultMarkdown);
        Assert.True(SlackConnectorConfig.DefaultUnfurlLinks);
        Assert.True(SlackConnectorConfig.DefaultUnfurlMedia);
        Assert.Equal(10, SlackConnectorConfig.DefaultBatchSize);
        Assert.Equal(3, SlackConnectorConfig.DefaultRetryCount);
        Assert.Equal(1000, SlackConnectorConfig.DefaultRetryDelayMs);
    }

    [Fact]
    public void SlackConnectorConfig_EventTypeConstants_AreCorrect()
    {
        Assert.Equal("message", SlackConnectorConfig.EventTypeMessage);
        Assert.Equal("reaction_added", SlackConnectorConfig.EventTypeReactionAdded);
        Assert.Equal("reaction_removed", SlackConnectorConfig.EventTypeReactionRemoved);
        Assert.Equal("channel_created", SlackConnectorConfig.EventTypeChannelCreated);
        Assert.Equal("channel_archive", SlackConnectorConfig.EventTypeChannelArchive);
        Assert.Equal("member_joined_channel", SlackConnectorConfig.EventTypeMemberJoined);
        Assert.Equal("member_left_channel", SlackConnectorConfig.EventTypeMemberLeft);
        Assert.Equal("app_mention", SlackConnectorConfig.EventTypeAppMention);
        Assert.Equal("file_shared", SlackConnectorConfig.EventTypeFileShared);
    }

    [Fact]
    public void SlackConnectorConfig_DefaultEventTypes_IncludesMessageAndAppMention()
    {
        var eventTypes = SlackConnectorConfig.DefaultEventTypes;
        Assert.Contains("message", eventTypes);
        Assert.Contains("app_mention", eventTypes);
    }

    [Fact]
    public void SlackConnectorConfig_OffsetKeys_AreCorrect()
    {
        Assert.Equal("ts", SlackConnectorConfig.OffsetTimestamp);
        Assert.Equal("channel", SlackConnectorConfig.OffsetChannel);
    }
}
