using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Instagram;

/// <summary>
/// Source connector that receives events from Instagram via Graph API webhooks.
/// </summary>
[ConnectorMetadata(
    Name = "instagram-source",
    Description = "Receives mentions and comments from Instagram via Graph API webhooks",
    Author = "Surgewave",
    Tags = "instagram,social,webhook,source")]
public sealed class InstagramSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(InstagramSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(InstagramConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Destination topic for Instagram events", EditorHint.Topic)
        .Define(InstagramConnectorConfig.BusinessAccountId, ConfigType.String, Importance.High,
            "Instagram Business Account ID")
        .Define(InstagramConnectorConfig.WebhookVerifyToken, ConfigType.Password, Importance.High,
            "Token for webhook verification")
        .Define(InstagramConnectorConfig.AppSecret, ConfigType.Password, "", Importance.High,
            "App secret used to validate the X-Hub-Signature-256 header on webhook events; unsigned or tampered requests are rejected (if empty, signatures are not validated)")
        .Define(InstagramConnectorConfig.WebhookPort, ConfigType.Int, InstagramConnectorConfig.DefaultWebhookPort, Importance.Medium,
            "Port for webhook HTTP server")
        .Define(InstagramConnectorConfig.WebhookPath, ConfigType.String, InstagramConnectorConfig.DefaultWebhookPath, Importance.Medium,
            "Path for webhook endpoint")
        .Define(InstagramConnectorConfig.IncludeComments, ConfigType.Boolean, InstagramConnectorConfig.DefaultIncludeComments, Importance.Low,
            "Include comment events")
        .Define(InstagramConnectorConfig.IncludeMentions, ConfigType.Boolean, InstagramConnectorConfig.DefaultIncludeMentions, Importance.Low,
            "Include mention events");

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
