using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.LinkedIn;

/// <summary>
/// Sink connector that posts to LinkedIn via Marketing API.
/// </summary>
[ConnectorMetadata(
    Name = "linkedin-sink",
    Description = "Posts content to LinkedIn via Marketing API",
    Author = "Surgewave",
    Tags = "linkedin,social,sink")]
public sealed class LinkedInSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(LinkedInSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(LinkedInConnectorConfig.AccessToken, ConfigType.Password, Importance.High,
            "LinkedIn API access token")
        .Define(LinkedInConnectorConfig.OrganizationId, ConfigType.String, Importance.Medium,
            "LinkedIn Organization ID (for company posts)")
        .Define(LinkedInConnectorConfig.PersonId, ConfigType.String, Importance.Medium,
            "LinkedIn Person ID (for personal posts)")
        .Define(LinkedInConnectorConfig.ApiVersion, ConfigType.String, LinkedInConnectorConfig.DefaultApiVersion, Importance.Medium,
            "API version")
        .Define(LinkedInConnectorConfig.TextField, ConfigType.String, "text", Importance.Medium,
            "JSON field containing post text")
        .Define(LinkedInConnectorConfig.DefaultVisibility, ConfigType.String, LinkedInConnectorConfig.DefaultVisibilityValue, Importance.Medium,
            "Default visibility: PUBLIC, CONNECTIONS", EditorHint.Select, options: ["PUBLIC", "CONNECTIONS"]);

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
