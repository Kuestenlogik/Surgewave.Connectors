using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Xmpp;

/// <summary>
/// Source connector that receives XMPP messages and presence updates.
/// </summary>
[ConnectorMetadata(
    Name = "xmpp-source",
    Description = "Receives XMPP messages and presence updates from Jabber/XMPP servers",
    Author = "Surgewave",
    Tags = "xmpp, jabber, chat, messaging, source")]
public sealed class XmppSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(XmppConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce XMPP messages to", EditorHint.Topic)
        .Define(XmppConnectorConfig.Host, ConfigType.String, Importance.High,
            "XMPP server hostname")
        .Define(XmppConnectorConfig.Port, ConfigType.Int,
            XmppConnectorConfig.DefaultPort.ToString(), Importance.Medium,
            "XMPP server port")
        .Define(XmppConnectorConfig.Domain, ConfigType.String, Importance.High,
            "XMPP domain (e.g., jabber.org)")
        .Define(XmppConnectorConfig.Username, ConfigType.String, Importance.High,
            "XMPP username (local part of JID)")
        .Define(XmppConnectorConfig.Password, ConfigType.Password, Importance.High,
            "XMPP password")
        .Define(XmppConnectorConfig.Resource, ConfigType.String,
            XmppConnectorConfig.DefaultResource, Importance.Low,
            "XMPP resource identifier")
        .Define(XmppConnectorConfig.UseTls, ConfigType.Boolean, "true", Importance.Medium,
            "Use TLS encryption")
        .Define(XmppConnectorConfig.IncludePresence, ConfigType.Boolean, "true", Importance.Medium,
            "Include presence stanzas")
        .Define(XmppConnectorConfig.IncludeGroupChat, ConfigType.Boolean, "true", Importance.Medium,
            "Include MUC group chat messages")
        .Define(XmppConnectorConfig.JoinRooms, ConfigType.List, "", Importance.Medium,
            "MUC rooms to join (comma-separated JIDs)")
        .Define(XmppConnectorConfig.FilterJids, ConfigType.List, "", Importance.Low,
            "JIDs to filter messages from (empty = all)");

    public override Type TaskClass => typeof(XmppSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(XmppConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{XmppConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(XmppConnectorConfig.Host, out var host) ||
            string.IsNullOrWhiteSpace(host))
        {
            throw new ArgumentException($"'{XmppConnectorConfig.Host}' is required");
        }

        if (!config.TryGetValue(XmppConnectorConfig.Domain, out var domain) ||
            string.IsNullOrWhiteSpace(domain))
        {
            throw new ArgumentException($"'{XmppConnectorConfig.Domain}' is required");
        }

        if (!config.TryGetValue(XmppConnectorConfig.Username, out var username) ||
            string.IsNullOrWhiteSpace(username))
        {
            throw new ArgumentException($"'{XmppConnectorConfig.Username}' is required");
        }

        if (!config.TryGetValue(XmppConnectorConfig.Password, out var password) ||
            string.IsNullOrWhiteSpace(password))
        {
            throw new ArgumentException($"'{XmppConnectorConfig.Password}' is required");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
