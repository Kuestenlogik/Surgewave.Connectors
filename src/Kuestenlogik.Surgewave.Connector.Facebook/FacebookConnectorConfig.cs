namespace Kuestenlogik.Surgewave.Connector.Facebook;

/// <summary>
/// Configuration constants for the Facebook connector.
/// </summary>
public static class FacebookConnectorConfig
{
    // Common
    public const string Topic = "topic";

    // API settings
    public const string AccessToken = "facebook.access.token";
    public const string PageId = "facebook.page.id";
    public const string ApiVersion = "facebook.api.version";
    public const string AppSecret = "facebook.app.secret";

    // Source settings (Webhook)
    public const string WebhookVerifyToken = "facebook.webhook.verify.token";
    public const string WebhookPort = "facebook.webhook.port";
    public const string WebhookPath = "facebook.webhook.path";
    public const string IncludeComments = "facebook.include.comments";
    public const string IncludeReactions = "facebook.include.reactions";
    public const string PollIntervalMs = "facebook.poll.interval.ms";

    // Sink settings
    public const string MessageField = "facebook.message.field";
    public const string LinkField = "facebook.link.field";
    public const string ImageUrlField = "facebook.image.url.field";
    public const string PostType = "facebook.post.type";

    // Defaults
    public const string DefaultApiVersion = "v18.0";
    public const string DefaultWebhookPath = "/webhook/facebook";
    public const int DefaultWebhookPort = 8081;
    public const int DefaultPollIntervalMs = 60000;
    public const bool DefaultIncludeComments = true;
    public const bool DefaultIncludeReactions = true;
    public const string BaseUrl = "https://graph.facebook.com";
}
