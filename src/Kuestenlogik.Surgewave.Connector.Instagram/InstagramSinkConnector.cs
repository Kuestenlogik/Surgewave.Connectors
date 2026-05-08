using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Instagram;

/// <summary>
/// Sink connector that publishes media to Instagram via Graph API.
/// </summary>
[ConnectorMetadata(
    Name = "instagram-sink",
    Description = "Publishes media and replies to Instagram via Graph API",
    Author = "Surgewave",
    Tags = "instagram,social,sink")]
public sealed class InstagramSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(InstagramSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(InstagramConnectorConfig.AccessToken, ConfigType.Password, Importance.High,
            "Instagram Graph API access token")
        .Define(InstagramConnectorConfig.BusinessAccountId, ConfigType.String, Importance.High,
            "Instagram Business Account ID")
        .Define(InstagramConnectorConfig.ApiVersion, ConfigType.String, InstagramConnectorConfig.DefaultApiVersion, Importance.Medium,
            "Graph API version")
        .Define(InstagramConnectorConfig.CaptionField, ConfigType.String, "caption", Importance.Medium,
            "JSON field containing post caption")
        .Define(InstagramConnectorConfig.ImageUrlField, ConfigType.String, "image_url", Importance.Medium,
            "JSON field containing image URL")
        .Define(InstagramConnectorConfig.MediaType, ConfigType.String, "image", Importance.Medium,
            "Media type: image, video, carousel", EditorHint.Select, options: ["image", "video", "carousel"]);

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
