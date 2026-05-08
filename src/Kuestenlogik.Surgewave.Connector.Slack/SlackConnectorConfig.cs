namespace Kuestenlogik.Surgewave.Connector.Slack;

/// <summary>
/// Configuration constants for the Slack connector.
/// </summary>
public static class SlackConnectorConfig
{
    // Common config
    public const string TopicConfig = "topic";
    public const string TopicsConfig = "topics";

    // Slack API settings
    public const string BotTokenConfig = "slack.bot.token";
    public const string AppTokenConfig = "slack.app.token";
    public const string SigningSecretConfig = "slack.signing.secret";

    // Source settings (Events API / Socket Mode)
    public const string SocketModeConfig = "slack.socket.mode";
    public const string EventTypesConfig = "slack.event.types";
    public const string ChannelFilterConfig = "slack.channel.filter";
    public const string UserFilterConfig = "slack.user.filter";
    public const string IncludeBotMessagesConfig = "slack.include.bot.messages";

    // Sink settings (Web API)
    public const string DefaultChannelConfig = "slack.default.channel";
    public const string ChannelFieldConfig = "slack.channel.field";
    public const string TextFieldConfig = "slack.text.field";
    public const string BlocksFieldConfig = "slack.blocks.field";
    public const string AttachmentsFieldConfig = "slack.attachments.field";
    public const string ThreadTsFieldConfig = "slack.thread.ts.field";
    public const string UsernameConfig = "slack.username";
    public const string IconEmojiConfig = "slack.icon.emoji";
    public const string IconUrlConfig = "slack.icon.url";
    public const string UnfurlLinksConfig = "slack.unfurl.links";
    public const string UnfurlMediaConfig = "slack.unfurl.media";

    // Message formatting
    public const string TextTemplateConfig = "slack.text.template";
    public const string MarkdownConfig = "slack.markdown";

    // Reactions
    public const string AddReactionConfig = "slack.add.reaction";
    public const string ReactionFieldConfig = "slack.reaction.field";
    public const string DefaultReactionConfig = "slack.default.reaction";

    // File uploads
    public const string FileFieldConfig = "slack.file.field";
    public const string FileNameFieldConfig = "slack.filename.field";
    public const string FileTitleFieldConfig = "slack.file.title.field";

    // Behavior settings
    public const string BatchSizeConfig = "slack.batch.size";
    public const string RetryCountConfig = "slack.retry.count";
    public const string RetryDelayMsConfig = "slack.retry.delay.ms";

    // Default values
    public const bool DefaultSocketMode = true;
    public const bool DefaultIncludeBotMessages = false;
    public const string DefaultTextTemplate = "${value}";
    public const bool DefaultMarkdown = true;
    public const bool DefaultUnfurlLinks = true;
    public const bool DefaultUnfurlMedia = true;
    public const int DefaultBatchSize = 10;
    public const int DefaultRetryCount = 3;
    public const int DefaultRetryDelayMs = 1000;

    // Event types
    public const string EventTypeMessage = "message";
    public const string EventTypeReactionAdded = "reaction_added";
    public const string EventTypeReactionRemoved = "reaction_removed";
    public const string EventTypeChannelCreated = "channel_created";
    public const string EventTypeChannelArchive = "channel_archive";
    public const string EventTypeMemberJoined = "member_joined_channel";
    public const string EventTypeMemberLeft = "member_left_channel";
    public const string EventTypeAppMention = "app_mention";
    public const string EventTypeFileShared = "file_shared";

    // Default event types to listen for
    public const string DefaultEventTypes = "message,app_mention";

    // Offset tracking
    public const string OffsetTimestamp = "ts";
    public const string OffsetChannel = "channel";
}
