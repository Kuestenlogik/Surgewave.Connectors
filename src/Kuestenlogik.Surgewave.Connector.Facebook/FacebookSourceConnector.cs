using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Facebook;

/// <summary>
/// Source connector that receives events from Facebook pages via webhooks or polling.
/// </summary>
[ConnectorMetadata(
    Name = "facebook-source",
    Description = "Receives events from Facebook pages via Graph API webhooks",
    Author = "Surgewave",
    Tags = "facebook,social,webhook,source")]
public sealed class FacebookSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(FacebookSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(FacebookConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Destination topic for Facebook events", EditorHint.Topic)
        .Define(FacebookConnectorConfig.AccessToken, ConfigType.Password, Importance.High,
            "Facebook Page access token")
        .Define(FacebookConnectorConfig.PageId, ConfigType.String, Importance.High,
            "Facebook Page ID to monitor")
        .Define(FacebookConnectorConfig.ApiVersion, ConfigType.String, FacebookConnectorConfig.DefaultApiVersion, Importance.Medium,
            "Graph API version")
        .Define(FacebookConnectorConfig.WebhookVerifyToken, ConfigType.Password, Importance.High,
            "Token for webhook verification")
        .Define(FacebookConnectorConfig.WebhookPort, ConfigType.Int, FacebookConnectorConfig.DefaultWebhookPort, Importance.Medium,
            "Port for webhook HTTP server")
        .Define(FacebookConnectorConfig.WebhookPath, ConfigType.String, FacebookConnectorConfig.DefaultWebhookPath, Importance.Medium,
            "Path for webhook endpoint")
        .Define(FacebookConnectorConfig.IncludeComments, ConfigType.Boolean, FacebookConnectorConfig.DefaultIncludeComments, Importance.Low,
            "Include post comments")
        .Define(FacebookConnectorConfig.IncludeReactions, ConfigType.Boolean, FacebookConnectorConfig.DefaultIncludeReactions, Importance.Low,
            "Include post reactions");

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
