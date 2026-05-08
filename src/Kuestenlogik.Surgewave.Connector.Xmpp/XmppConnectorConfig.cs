namespace Kuestenlogik.Surgewave.Connector.Xmpp;

/// <summary>
/// Configuration constants for XMPP connector.
/// </summary>
public static class XmppConnectorConfig
{
    // Connection settings
    public const string Host = "xmpp.host";
    public const string Port = "xmpp.port";
    public const string Domain = "xmpp.domain";
    public const string Username = "xmpp.username";
    public const string Password = "xmpp.password";
    public const string Resource = "xmpp.resource";
    public const string UseTls = "xmpp.use.tls";

    // Source settings
    public const string Topic = "topic";
    public const string IncludePresence = "xmpp.include.presence";
    public const string IncludeGroupChat = "xmpp.include.groupchat";
    public const string JoinRooms = "xmpp.join.rooms";  // Comma-separated MUC rooms
    public const string FilterJids = "xmpp.filter.jids";  // Comma-separated JIDs to monitor

    // Sink settings
    public const string Topics = "topics";
    public const string DefaultRecipient = "xmpp.default.recipient";
    public const string MessageType = "xmpp.message.type";  // chat, groupchat, normal

    // Defaults
    public const int DefaultPort = 5222;
    public const string DefaultResource = "surgewave-connector";
    public const string DefaultMessageType = "chat";
}
