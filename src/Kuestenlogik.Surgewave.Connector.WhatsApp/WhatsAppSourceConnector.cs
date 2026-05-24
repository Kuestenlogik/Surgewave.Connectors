using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.WhatsApp;

/// <summary>
/// Source connector that receives messages from WhatsApp via webhook.
/// </summary>
[ConnectorMetadata(
    Name = "whatsapp-source",
    Description = "Receives messages from WhatsApp Business API via webhooks",
    Author = "Surgewave",
    Tags = "whatsapp,messaging,webhook,source")]
public sealed class WhatsAppSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(WhatsAppSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(WhatsAppConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Destination topic for WhatsApp messages", EditorHint.Topic)
        .Define(WhatsAppConnectorConfig.AccessToken, ConfigType.Password, Importance.High,
            "WhatsApp Business API access token")
        .Define(WhatsAppConnectorConfig.WebhookVerifyToken, ConfigType.Password, Importance.High,
            "Token for webhook verification")
        .Define(WhatsAppConnectorConfig.WebhookPort, ConfigType.Int, WhatsAppConnectorConfig.DefaultWebhookPort, Importance.Medium,
            "Port for webhook HTTP server")
        .Define(WhatsAppConnectorConfig.WebhookPath, ConfigType.String, WhatsAppConnectorConfig.DefaultWebhookPath, Importance.Medium,
            "Path for webhook endpoint");

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
