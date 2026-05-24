namespace Kuestenlogik.Surgewave.Connector.WhatsApp;

/// <summary>
/// Configuration constants for the WhatsApp Business Cloud API connector.
/// </summary>
public static class WhatsAppConnectorConfig
{
    // Common
    public const string Topic = "topic";

    // API settings
    public const string AccessToken = "whatsapp.access.token";
    public const string PhoneNumberId = "whatsapp.phone.number.id";
    public const string BusinessAccountId = "whatsapp.business.account.id";
    public const string ApiVersion = "whatsapp.api.version";
    public const string WebhookVerifyToken = "whatsapp.webhook.verify.token";

    // Source settings (Webhook)
    public const string WebhookPort = "whatsapp.webhook.port";
    public const string WebhookPath = "whatsapp.webhook.path";

    // Sink settings
    public const string DefaultRecipient = "whatsapp.default.recipient";
    public const string RecipientField = "whatsapp.recipient.field";
    public const string MessageField = "whatsapp.message.field";
    public const string TemplateNameField = "whatsapp.template.name.field";
    public const string TemplateLanguageField = "whatsapp.template.language.field";
    public const string MessageType = "whatsapp.message.type";

    // Defaults
    public const string DefaultApiVersion = "v18.0";
    public const string DefaultWebhookPath = "/webhook/whatsapp";
    public const int DefaultWebhookPort = 8080;
    public const string DefaultMessageType = "text";
    public const string BaseUrl = "https://graph.facebook.com";
}
