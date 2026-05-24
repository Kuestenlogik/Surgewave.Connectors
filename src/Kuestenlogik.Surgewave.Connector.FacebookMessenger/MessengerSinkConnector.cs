using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.FacebookMessenger;

/// <summary>
/// Sink connector that sends messages via Facebook Messenger Platform API.
/// </summary>
[ConnectorMetadata(
    Name = "messenger-sink",
    Description = "Sends messages via Facebook Messenger Platform API",
    Author = "Surgewave",
    Tags = "facebook,messenger,messaging,sink")]
public sealed class MessengerSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(MessengerSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(MessengerConnectorConfig.PageAccessToken, ConfigType.Password, Importance.High,
            "Facebook Page access token")
        .Define(MessengerConnectorConfig.ApiVersion, ConfigType.String, MessengerConnectorConfig.DefaultApiVersion, Importance.Medium,
            "Graph API version")
        .Define(MessengerConnectorConfig.DefaultRecipientId, ConfigType.String, Importance.Medium,
            "Default recipient PSID")
        .Define(MessengerConnectorConfig.RecipientIdField, ConfigType.String, "recipient_id", Importance.Medium,
            "JSON field containing recipient PSID")
        .Define(MessengerConnectorConfig.MessageTextField, ConfigType.String, "text", Importance.Medium,
            "JSON field containing message text")
        .Define(MessengerConnectorConfig.MessageType, ConfigType.String, MessengerConnectorConfig.DefaultMessageType, Importance.Medium,
            "Message type: text, template, quick_replies");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
