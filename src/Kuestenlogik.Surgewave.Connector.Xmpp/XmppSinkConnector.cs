using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Xmpp;

/// <summary>
/// Sink connector that sends XMPP messages.
/// </summary>
[ConnectorMetadata(
    Name = "xmpp-sink",
    Description = "Sends messages to XMPP/Jabber recipients",
    Author = "Surgewave",
    Tags = "xmpp, jabber, chat, messaging, sink")]
public sealed class XmppSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(XmppConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume messages from", EditorHint.Topic)
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
        .Define(XmppConnectorConfig.DefaultRecipient, ConfigType.String, "", Importance.Medium,
            "Default recipient JID when not specified in message")
        .Define(XmppConnectorConfig.MessageType, ConfigType.String,
            XmppConnectorConfig.DefaultMessageType, Importance.Low,
            "Default message type (chat, groupchat, normal)");

    public override Type TaskClass => typeof(XmppSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(XmppConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{XmppConnectorConfig.Topics}' is required");
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
