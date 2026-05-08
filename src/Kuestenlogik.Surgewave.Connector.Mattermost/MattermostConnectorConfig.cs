namespace Kuestenlogik.Surgewave.Connector.Mattermost;

/// <summary>
/// Configuration constants for Mattermost connector.
/// </summary>
public static class MattermostConnectorConfig
{
    public const string ServerUrl = "mattermost.server.url";
    public const string AccessToken = "mattermost.access.token";
    public const string Topic = "topic";
    public const string ChannelIds = "mattermost.channel.ids";
    public const string ChannelId = "mattermost.channel.id";
    public const string IncludeBotMessages = "mattermost.include.bot.messages";
    public const string PollIntervalMs = "mattermost.poll.interval.ms";
    public const string MessageField = "mattermost.message.field";

    // Defaults
    public const string DefaultServerUrl = "https://mattermost.example.com";
    public const bool DefaultIncludeBotMessages = false;
    public const int DefaultPollIntervalMs = 5000;
    public const string DefaultMessageField = "message";
}
