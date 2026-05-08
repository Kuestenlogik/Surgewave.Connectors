namespace Kuestenlogik.Surgewave.Connector.Teams;

/// <summary>
/// Configuration constants for the Microsoft Teams connector.
/// </summary>
public static class TeamsConnectorConfig
{
    // Authentication
    public const string TenantId = "teams.tenant.id";
    public const string ClientId = "teams.client.id";
    public const string ClientSecret = "teams.client.secret";

    // Common
    public const string Topic = "topic";
    public const string Topics = "topics";
    public const string TeamId = "teams.team.id";
    public const string ChannelId = "teams.channel.id";
    public const string ChatId = "teams.chat.id";

    // Sink
    public const string MessageFormat = "message.format";
    public const string DefaultSubject = "message.default.subject";

    // Source
    public const string PollIntervalMs = "poll.interval.ms";
    public const string WebhookUrl = "webhook.url";
    public const string WebhookSecret = "webhook.secret";
    public const string IncludeReplies = "include.replies";

    // Message format values
    public const string FormatText = "text";
    public const string FormatHtml = "html";
    public const string FormatAdaptiveCard = "adaptivecard";

    // Defaults
    public const string DefaultMessageFormat = FormatText;
    public const int DefaultPollIntervalMs = 5000;
    public const bool DefaultIncludeReplies = false;
}
