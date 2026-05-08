using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.FacebookMessenger;

/// <summary>
/// Source connector that receives messages from Facebook Messenger via webhooks.
/// </summary>
[ConnectorMetadata(
    Name = "messenger-source",
    Description = "Receives messages from Facebook Messenger via webhooks",
    Author = "Surgewave",
    Tags = "facebook,messenger,messaging,webhook,source")]
public sealed class MessengerSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(MessengerSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(MessengerConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Destination topic for Messenger messages", EditorHint.Topic)
        .Define(MessengerConnectorConfig.PageAccessToken, ConfigType.Password, Importance.High,
            "Facebook Page access token")
        .Define(MessengerConnectorConfig.AppSecret, ConfigType.Password, Importance.Medium,
            "Facebook App secret for signature verification")
        .Define(MessengerConnectorConfig.WebhookVerifyToken, ConfigType.Password, Importance.High,
            "Token for webhook verification")
        .Define(MessengerConnectorConfig.WebhookPort, ConfigType.Int, MessengerConnectorConfig.DefaultWebhookPort, Importance.Medium,
            "Port for webhook HTTP server")
        .Define(MessengerConnectorConfig.WebhookPath, ConfigType.String, MessengerConnectorConfig.DefaultWebhookPath, Importance.Medium,
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
