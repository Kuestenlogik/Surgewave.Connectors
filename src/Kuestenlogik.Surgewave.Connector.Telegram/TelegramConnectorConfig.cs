namespace Kuestenlogik.Surgewave.Connector.Telegram;

/// <summary>
/// Configuration constants for Telegram connector.
/// </summary>
public static class TelegramConnectorConfig
{
    // Authentication
    public const string BotToken = "telegram.bot.token";

    // Source settings
    public const string Topic = "topic";
    public const string ChatIds = "telegram.chat.ids";
    public const string IncludeGroups = "telegram.include.groups";
    public const string IncludeChannels = "telegram.include.channels";
    public const string IncludePrivate = "telegram.include.private";
    public const string MessageTypes = "telegram.message.types";  // text, photo, video, document, all
    public const string PollingMode = "telegram.polling.mode";  // long-polling, webhook

    // Sink settings
    public const string Topics = "topics";
    public const string DefaultChatId = "telegram.default.chat.id";
    public const string ParseMode = "telegram.parse.mode";  // Markdown, MarkdownV2, HTML
    public const string DisableNotification = "telegram.disable.notification";
    public const string DisableWebPagePreview = "telegram.disable.web.page.preview";

    // Webhook settings
    public const string WebhookUrl = "telegram.webhook.url";
    public const string WebhookPort = "telegram.webhook.port";

    // Defaults
    public const string DefaultMessageTypes = "all";
    public const string DefaultPollingMode = "long-polling";
    public const string DefaultParseMode = "Markdown";
}
