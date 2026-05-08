using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.LinkedIn;

/// <summary>
/// Source connector that receives events from LinkedIn via webhooks.
/// </summary>
[ConnectorMetadata(
    Name = "linkedin-source",
    Description = "Receives shares and mentions from LinkedIn via Marketing API webhooks",
    Author = "Surgewave",
    Tags = "linkedin,social,webhook,source")]
public sealed class LinkedInSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(LinkedInSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(LinkedInConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Destination topic for LinkedIn events", EditorHint.Topic)
        .Define(LinkedInConnectorConfig.AccessToken, ConfigType.Password, Importance.High,
            "LinkedIn API access token")
        .Define(LinkedInConnectorConfig.OrganizationId, ConfigType.String, Importance.Medium,
            "LinkedIn Organization ID (for company pages)")
        .Define(LinkedInConnectorConfig.PersonId, ConfigType.String, Importance.Medium,
            "LinkedIn Person ID (for personal profiles)")
        .Define(LinkedInConnectorConfig.WebhookVerifyToken, ConfigType.Password, Importance.High,
            "Token for webhook verification")
        .Define(LinkedInConnectorConfig.WebhookPort, ConfigType.Int, LinkedInConnectorConfig.DefaultWebhookPort, Importance.Medium,
            "Port for webhook HTTP server")
        .Define(LinkedInConnectorConfig.WebhookPath, ConfigType.String, LinkedInConnectorConfig.DefaultWebhookPath, Importance.Medium,
            "Path for webhook endpoint")
        .Define(LinkedInConnectorConfig.IncludeShares, ConfigType.Boolean, LinkedInConnectorConfig.DefaultIncludeShares, Importance.Low,
            "Include share events")
        .Define(LinkedInConnectorConfig.IncludeMentions, ConfigType.Boolean, LinkedInConnectorConfig.DefaultIncludeMentions, Importance.Low,
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
