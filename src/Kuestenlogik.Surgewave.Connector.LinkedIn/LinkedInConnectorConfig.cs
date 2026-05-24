namespace Kuestenlogik.Surgewave.Connector.LinkedIn;

/// <summary>
/// Configuration constants for the LinkedIn Marketing API connector.
/// </summary>
public static class LinkedInConnectorConfig
{
    // Common
    public const string Topic = "topic";

    // API settings
    public const string AccessToken = "linkedin.access.token";
    public const string OrganizationId = "linkedin.organization.id";
    public const string PersonId = "linkedin.person.id";
    public const string ApiVersion = "linkedin.api.version";

    // Source settings (Webhook/Polling)
    public const string WebhookVerifyToken = "linkedin.webhook.verify.token";
    public const string WebhookPort = "linkedin.webhook.port";
    public const string WebhookPath = "linkedin.webhook.path";
    public const string PollIntervalMs = "linkedin.poll.interval.ms";
    public const string IncludeShares = "linkedin.include.shares";
    public const string IncludeMentions = "linkedin.include.mentions";

    // Sink settings
    public const string TextField = "linkedin.text.field";
    public const string MediaField = "linkedin.media.field";
    public const string VisibilityField = "linkedin.visibility.field";
    public const string PostType = "linkedin.post.type";
    public const string DefaultVisibility = "linkedin.default.visibility";

    // Defaults
    public const string DefaultApiVersion = "v2";
    public const string DefaultWebhookPath = "/webhook/linkedin";
    public const int DefaultWebhookPort = 8084;
    public const int DefaultPollIntervalMs = 60000;
    public const bool DefaultIncludeShares = true;
    public const bool DefaultIncludeMentions = true;
    public const string DefaultVisibilityValue = "PUBLIC";
    public const string BaseUrl = "https://api.linkedin.com";
}
