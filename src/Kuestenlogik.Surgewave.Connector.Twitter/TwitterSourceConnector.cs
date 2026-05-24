using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Twitter;

/// <summary>
/// Source connector that reads tweets from Twitter/X via API v2.
/// </summary>
[ConnectorMetadata(
    Name = "twitter-source",
    Description = "Reads tweets via Twitter/X API v2 (search, user timeline, filtered stream)",
    Author = "Surgewave",
    Tags = "twitter,x,social,source")]
public sealed class TwitterSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(TwitterSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(TwitterConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Destination topic for tweets", EditorHint.Topic)
        .Define(TwitterConnectorConfig.BearerToken, ConfigType.Password, Importance.High,
            "Twitter API v2 Bearer token")
        .Define(TwitterConnectorConfig.ConsumerKey, ConfigType.Password, Importance.Medium,
            "Twitter API consumer key (for user context)")
        .Define(TwitterConnectorConfig.ConsumerSecret, ConfigType.Password, Importance.Medium,
            "Twitter API consumer secret")
        .Define(TwitterConnectorConfig.AccessToken, ConfigType.Password, Importance.Medium,
            "Twitter API access token")
        .Define(TwitterConnectorConfig.AccessTokenSecret, ConfigType.Password, Importance.Medium,
            "Twitter API access token secret")
        .Define(TwitterConnectorConfig.SearchQuery, ConfigType.String, Importance.Medium,
            "Search query for recent tweets")
        .Define(TwitterConnectorConfig.UserIds, ConfigType.String, Importance.Medium,
            "Comma-separated user IDs to monitor")
        .Define(TwitterConnectorConfig.PollIntervalMs, ConfigType.Int, TwitterConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(TwitterConnectorConfig.MaxResults, ConfigType.Int, TwitterConnectorConfig.DefaultMaxResults, Importance.Low,
            "Maximum results per request")
        .Define(TwitterConnectorConfig.IncludeRetweets, ConfigType.Boolean, TwitterConnectorConfig.DefaultIncludeRetweets, Importance.Low,
            "Include retweets")
        .Define(TwitterConnectorConfig.IncludeReplies, ConfigType.Boolean, TwitterConnectorConfig.DefaultIncludeReplies, Importance.Low,
            "Include replies");

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
