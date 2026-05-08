namespace Kuestenlogik.Surgewave.Connector.FacebookMessenger;

/// <summary>
/// Configuration constants for the Facebook Messenger connector.
/// </summary>
public static class MessengerConnectorConfig
{
    // Common
    public const string Topic = "topic";

    // API settings
    public const string PageAccessToken = "messenger.page.access.token";
    public const string AppSecret = "messenger.app.secret";
    public const string ApiVersion = "messenger.api.version";

    // Source settings (Webhook)
    public const string WebhookVerifyToken = "messenger.webhook.verify.token";
    public const string WebhookPort = "messenger.webhook.port";
    public const string WebhookPath = "messenger.webhook.path";

    // Sink settings
    public const string DefaultRecipientId = "messenger.default.recipient.id";
    public const string RecipientIdField = "messenger.recipient.id.field";
    public const string MessageTextField = "messenger.message.text.field";
    public const string MessageType = "messenger.message.type";
    public const string QuickRepliesField = "messenger.quick.replies.field";
    public const string TemplateTypeField = "messenger.template.type.field";

    // Defaults
    public const string DefaultApiVersion = "v18.0";
    public const string DefaultWebhookPath = "/webhook/messenger";
    public const int DefaultWebhookPort = 8082;
    public const string DefaultMessageType = "text";
    public const string BaseUrl = "https://graph.facebook.com";
}
