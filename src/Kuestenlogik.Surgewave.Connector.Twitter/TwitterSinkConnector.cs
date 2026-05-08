using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Twitter;

/// <summary>
/// Sink connector that posts tweets to Twitter/X via API v2.
/// </summary>
[ConnectorMetadata(
    Name = "twitter-sink",
    Description = "Posts tweets via Twitter/X API v2",
    Author = "Surgewave",
    Tags = "twitter,x,social,sink")]
public sealed class TwitterSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(TwitterSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(TwitterConnectorConfig.ConsumerKey, ConfigType.Password, Importance.High,
            "Twitter API consumer key")
        .Define(TwitterConnectorConfig.ConsumerSecret, ConfigType.Password, Importance.High,
            "Twitter API consumer secret")
        .Define(TwitterConnectorConfig.AccessToken, ConfigType.Password, Importance.High,
            "Twitter API access token")
        .Define(TwitterConnectorConfig.AccessTokenSecret, ConfigType.Password, Importance.High,
            "Twitter API access token secret")
        .Define(TwitterConnectorConfig.TextField, ConfigType.String, "text", Importance.Medium,
            "JSON field containing tweet text")
        .Define(TwitterConnectorConfig.ReplyToField, ConfigType.String, Importance.Low,
            "JSON field containing reply-to tweet ID")
        .Define(TwitterConnectorConfig.QuoteTweetField, ConfigType.String, Importance.Low,
            "JSON field containing quote tweet ID");

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
