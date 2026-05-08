namespace Kuestenlogik.Surgewave.Connector.RocketChat;

/// <summary>
/// Configuration constants for the Rocket.Chat connector.
/// </summary>
public static class RocketChatConnectorConfig
{
    // Common
    public const string Topic = "topic";

    // Connection
    public const string ServerUrl = "rocketchat.server.url";
    public const string UserId = "rocketchat.user.id";
    public const string AuthToken = "rocketchat.auth.token";
    public const string Username = "rocketchat.username";
    public const string Password = "rocketchat.password";

    // Source settings
    public const string RoomIds = "rocketchat.room.ids";
    public const string IncludeBotMessages = "rocketchat.include.bot.messages";
    public const string PollIntervalMs = "rocketchat.poll.interval.ms";

    // Sink settings
    public const string DefaultRoomId = "rocketchat.default.room.id";
    public const string RoomIdField = "rocketchat.room.id.field";
    public const string TextField = "rocketchat.text.field";
    public const string AliasField = "rocketchat.alias.field";
    public const string EmojiField = "rocketchat.emoji.field";
    public const string AvatarField = "rocketchat.avatar.field";

    // Defaults
    public const string DefaultServerUrl = "http://localhost:3000";
    public const int DefaultPollIntervalMs = 1000;
    public const bool DefaultIncludeBotMessages = false;
}
