using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Facebook;

/// <summary>
/// Sink connector that posts to Facebook pages via Graph API.
/// </summary>
[ConnectorMetadata(
    Name = "facebook-sink",
    Description = "Posts to Facebook pages via Graph API",
    Author = "Surgewave",
    Tags = "facebook,social,sink")]
public sealed class FacebookSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(FacebookSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(FacebookConnectorConfig.AccessToken, ConfigType.Password, Importance.High,
            "Facebook Page access token")
        .Define(FacebookConnectorConfig.PageId, ConfigType.String, Importance.High,
            "Facebook Page ID")
        .Define(FacebookConnectorConfig.ApiVersion, ConfigType.String, FacebookConnectorConfig.DefaultApiVersion, Importance.Medium,
            "Graph API version")
        .Define(FacebookConnectorConfig.MessageField, ConfigType.String, "message", Importance.Medium,
            "JSON field containing post message")
        .Define(FacebookConnectorConfig.LinkField, ConfigType.String, Importance.Low,
            "JSON field containing link URL")
        .Define(FacebookConnectorConfig.PostType, ConfigType.String, "feed", Importance.Medium,
            "Post type: feed, photo, video", EditorHint.Select, options: ["feed", "photo", "video"]);

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
