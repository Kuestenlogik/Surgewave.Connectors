using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Reddit;

/// <summary>
/// Sink connector that posts content to subreddits.
/// </summary>
[ConnectorMetadata(
    Name = "Reddit Sink",
    Description = "Posts content and comments to subreddits on Reddit",
    Version = "1.0.0",
    Author = "KL Surgewave",
    DocumentationUrl = "https://www.reddit.com/dev/api/",
    Tags = "reddit,social,posts,comments,sink")]
public class RedditSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(RedditSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Topics
        .Define(RedditConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)

        // Reddit API credentials
        .Define(RedditConnectorConfig.ClientIdConfig, ConfigType.String, Importance.High,
            "Reddit OAuth client ID")
        .Define(RedditConnectorConfig.ClientSecretConfig, ConfigType.Password, Importance.High,
            "Reddit OAuth client secret")
        .Define(RedditConnectorConfig.UsernameConfig, ConfigType.String, Importance.High,
            "Reddit username for authentication")
        .Define(RedditConnectorConfig.PasswordConfig, ConfigType.Password, Importance.High,
            "Reddit password for authentication")
        .Define(RedditConnectorConfig.UserAgentConfig, ConfigType.String, Importance.High,
            "User agent string for Reddit API requests (e.g., 'MyApp/1.0 by username')")

        // Subreddit settings
        .Define(RedditConnectorConfig.DefaultSubredditConfig, ConfigType.String, Importance.High,
            "Default subreddit to post to (without r/ prefix)")
        .Define(RedditConnectorConfig.SubredditFieldConfig, ConfigType.String, Importance.Medium,
            "JSON field in record value containing target subreddit (overrides default)")

        // Post content
        .Define(RedditConnectorConfig.TitleFieldConfig, ConfigType.String, Importance.Medium,
            "JSON field containing post title")
        .Define(RedditConnectorConfig.TextFieldConfig, ConfigType.String, Importance.Medium,
            "JSON field containing post/comment text body")
        .Define(RedditConnectorConfig.UrlFieldConfig, ConfigType.String, Importance.Low,
            "JSON field containing URL for link posts")
        .Define(RedditConnectorConfig.FlairFieldConfig, ConfigType.String, Importance.Low,
            "JSON field containing flair ID to apply")
        .Define(RedditConnectorConfig.NsfwFieldConfig, ConfigType.String, Importance.Low,
            "JSON field containing NSFW flag (boolean)")
        .Define(RedditConnectorConfig.SpoilerFieldConfig, ConfigType.String, Importance.Low,
            "JSON field containing spoiler flag (boolean)")

        // Post type
        .Define(RedditConnectorConfig.PostTypeConfig, ConfigType.String, RedditConnectorConfig.DefaultPostType, Importance.Medium,
            "Type of post to create: self, link, or crosspost", EditorHint.Select, options: ["self", "link"])

        // Comment settings
        .Define(RedditConnectorConfig.ParentIdFieldConfig, ConfigType.String, Importance.Medium,
            "JSON field containing parent post/comment ID for replies")
        .Define(RedditConnectorConfig.ReplyToCommentsConfig, ConfigType.Boolean, RedditConnectorConfig.DefaultReplyToComments, Importance.Medium,
            "Create comments instead of posts (requires parent_id)")

        // Behavior
        .Define(RedditConnectorConfig.BatchSizeConfig, ConfigType.Int, RedditConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Maximum number of posts to send per batch")
        .Define(RedditConnectorConfig.RetryCountConfig, ConfigType.Int, RedditConnectorConfig.DefaultRetryCount, Importance.Low,
            "Number of retries for failed API calls")
        .Define(RedditConnectorConfig.RetryDelayMsConfig, ConfigType.Int, RedditConnectorConfig.DefaultRetryDelayMs, Importance.Low,
            "Delay between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Validate required config
        if (!config.TryGetValue(RedditConnectorConfig.TopicsConfig, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"Missing required configuration: {RedditConnectorConfig.TopicsConfig}");
        }

        if (!config.TryGetValue(RedditConnectorConfig.ClientIdConfig, out var clientId) ||
            string.IsNullOrWhiteSpace(clientId))
        {
            throw new ArgumentException($"Missing required configuration: {RedditConnectorConfig.ClientIdConfig}");
        }

        if (!config.TryGetValue(RedditConnectorConfig.ClientSecretConfig, out var clientSecret) ||
            string.IsNullOrWhiteSpace(clientSecret))
        {
            throw new ArgumentException($"Missing required configuration: {RedditConnectorConfig.ClientSecretConfig}");
        }

        if (!config.TryGetValue(RedditConnectorConfig.UsernameConfig, out var username) ||
            string.IsNullOrWhiteSpace(username))
        {
            throw new ArgumentException($"Missing required configuration: {RedditConnectorConfig.UsernameConfig}");
        }

        if (!config.TryGetValue(RedditConnectorConfig.PasswordConfig, out var password) ||
            string.IsNullOrWhiteSpace(password))
        {
            throw new ArgumentException($"Missing required configuration: {RedditConnectorConfig.PasswordConfig}");
        }

        if (!config.TryGetValue(RedditConnectorConfig.UserAgentConfig, out var userAgent) ||
            string.IsNullOrWhiteSpace(userAgent))
        {
            throw new ArgumentException($"Missing required configuration: {RedditConnectorConfig.UserAgentConfig}");
        }

        // Require either default subreddit or subreddit field for posting
        var hasDefaultSubreddit = config.TryGetValue(RedditConnectorConfig.DefaultSubredditConfig, out var defaultSubreddit) &&
                                  !string.IsNullOrWhiteSpace(defaultSubreddit);
        var hasSubredditField = config.TryGetValue(RedditConnectorConfig.SubredditFieldConfig, out var subredditField) &&
                                !string.IsNullOrWhiteSpace(subredditField);
        var replyToComments = config.TryGetValue(RedditConnectorConfig.ReplyToCommentsConfig, out var replyStr) &&
                              bool.TryParse(replyStr, out var reply) && reply;

        if (!replyToComments && !hasDefaultSubreddit && !hasSubredditField)
        {
            throw new ArgumentException(
                $"Either {RedditConnectorConfig.DefaultSubredditConfig} or {RedditConnectorConfig.SubredditFieldConfig} must be configured for posting");
        }
    }

    public override void Stop()
    {
        // No cleanup needed
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task per connector
        return [new Dictionary<string, string>(_config)];
    }
}
