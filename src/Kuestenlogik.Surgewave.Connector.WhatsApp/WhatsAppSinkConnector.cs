using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.WhatsApp;

/// <summary>
/// Sink connector that sends messages via WhatsApp Business Cloud API.
/// </summary>
[ConnectorMetadata(
    Name = "whatsapp-sink",
    Description = "Sends messages via WhatsApp Business Cloud API",
    Author = "Surgewave",
    Tags = "whatsapp,messaging,sink")]
public sealed class WhatsAppSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(WhatsAppSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(WhatsAppConnectorConfig.AccessToken, ConfigType.Password, Importance.High,
            "WhatsApp Business API access token")
        .Define(WhatsAppConnectorConfig.PhoneNumberId, ConfigType.String, Importance.High,
            "WhatsApp Business phone number ID")
        .Define(WhatsAppConnectorConfig.ApiVersion, ConfigType.String, WhatsAppConnectorConfig.DefaultApiVersion, Importance.Medium,
            "Graph API version")
        .Define(WhatsAppConnectorConfig.DefaultRecipient, ConfigType.String, Importance.Medium,
            "Default recipient phone number")
        .Define(WhatsAppConnectorConfig.RecipientField, ConfigType.String, "to", Importance.Medium,
            "JSON field containing recipient phone number")
        .Define(WhatsAppConnectorConfig.MessageField, ConfigType.String, "text", Importance.Medium,
            "JSON field containing message text")
        .Define(WhatsAppConnectorConfig.MessageType, ConfigType.String, WhatsAppConnectorConfig.DefaultMessageType, Importance.Medium,
            "Message type: text, template");

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
