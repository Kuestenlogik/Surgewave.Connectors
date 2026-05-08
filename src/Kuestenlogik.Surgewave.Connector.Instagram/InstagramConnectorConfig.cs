namespace Kuestenlogik.Surgewave.Connector.Instagram;

/// <summary>
/// Configuration constants for the Instagram Graph API connector.
/// </summary>
public static class InstagramConnectorConfig
{
    // Common
    public const string Topic = "topic";

    // API settings
    public const string AccessToken = "instagram.access.token";
    public const string BusinessAccountId = "instagram.business.account.id";
    public const string ApiVersion = "instagram.api.version";
    public const string AppSecret = "instagram.app.secret";

    // Source settings (Webhook/Polling)
    public const string WebhookVerifyToken = "instagram.webhook.verify.token";
    public const string WebhookPort = "instagram.webhook.port";
    public const string WebhookPath = "instagram.webhook.path";
    public const string PollIntervalMs = "instagram.poll.interval.ms";
    public const string IncludeComments = "instagram.include.comments";
    public const string IncludeMentions = "instagram.include.mentions";

    // Sink settings
    public const string CaptionField = "instagram.caption.field";
    public const string ImageUrlField = "instagram.image.url.field";
    public const string VideoUrlField = "instagram.video.url.field";
    public const string MediaType = "instagram.media.type";

    // Defaults
    public const string DefaultApiVersion = "v18.0";
    public const string DefaultWebhookPath = "/webhook/instagram";
    public const int DefaultWebhookPort = 8083;
    public const int DefaultPollIntervalMs = 60000;
    public const bool DefaultIncludeComments = true;
    public const bool DefaultIncludeMentions = true;
    public const string BaseUrl = "https://graph.facebook.com";
}
