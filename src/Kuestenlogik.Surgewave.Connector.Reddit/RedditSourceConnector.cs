using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Reddit;

/// <summary>
/// Source connector that polls subreddits for new posts and comments.
/// </summary>
[ConnectorMetadata(
    Name = "Reddit Source",
    Description = "Polls subreddits for new posts and comments from Reddit",
    Version = "1.0.0",
    Author = "KL Surgewave",
    DocumentationUrl = "https://www.reddit.com/dev/api/",
    Tags = "reddit,social,posts,comments,source")]
public class RedditSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(RedditSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Topic
        .Define(RedditConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination topic for Reddit posts and comments", EditorHint.Topic)

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
        .Define(RedditConnectorConfig.SubredditsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of subreddits to poll (without r/ prefix)")

        // Polling settings
        .Define(RedditConnectorConfig.PollIntervalMsConfig, ConfigType.Int, RedditConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Polling interval in milliseconds")
        .Define(RedditConnectorConfig.ContentTypeConfig, ConfigType.String, RedditConnectorConfig.DefaultContentType, Importance.Medium,
            "Content type to poll: posts, comments, or both", EditorHint.Select, options: ["posts", "comments", "all"])
        .Define(RedditConnectorConfig.SortByConfig, ConfigType.String, RedditConnectorConfig.DefaultSortBy, Importance.Medium,
            "Sort order for posts: hot, new, top, rising, controversial", EditorHint.Select, options: ["hot", "new", "top", "rising"])
        .Define(RedditConnectorConfig.TimePeriodConfig, ConfigType.String, RedditConnectorConfig.DefaultTimePeriod, Importance.Low,
            "Time period for 'top' and 'controversial' sorts: hour, day, week, month, year, all", EditorHint.Select, options: ["hour", "day", "week", "month", "year", "all"])
        .Define(RedditConnectorConfig.MaxPostsPerPollConfig, ConfigType.Int, RedditConnectorConfig.DefaultMaxPostsPerPoll, Importance.Medium,
            "Maximum number of posts to fetch per poll")

        // Comment settings
        .Define(RedditConnectorConfig.IncludeCommentsConfig, ConfigType.Boolean, RedditConnectorConfig.DefaultIncludeComments, Importance.Medium,
            "Include comments from fetched posts")
        .Define(RedditConnectorConfig.MaxCommentsPerPostConfig, ConfigType.Int, RedditConnectorConfig.DefaultMaxCommentsPerPost, Importance.Low,
            "Maximum number of comments to fetch per post")

        // Filters
        .Define(RedditConnectorConfig.FlairFilterConfig, ConfigType.String, Importance.Low,
            "Comma-separated list of flairs to filter by (empty = all)")
        .Define(RedditConnectorConfig.NsfwFilterConfig, ConfigType.String, RedditConnectorConfig.DefaultNsfwFilter, Importance.Low,
            "NSFW content filter: include, exclude, or only", EditorHint.Select, options: ["include", "exclude", "only"]);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Validate required config
        if (!config.TryGetValue(RedditConnectorConfig.TopicConfig, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"Missing required configuration: {RedditConnectorConfig.TopicConfig}");
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

        if (!config.TryGetValue(RedditConnectorConfig.SubredditsConfig, out var subreddits) ||
            string.IsNullOrWhiteSpace(subreddits))
        {
            throw new ArgumentException($"Missing required configuration: {RedditConnectorConfig.SubredditsConfig}");
        }
    }

    public override void Stop()
    {
        // No cleanup needed
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Can distribute subreddits across tasks
        if (!_config.TryGetValue(RedditConnectorConfig.SubredditsConfig, out var subredditsStr))
        {
            return [new Dictionary<string, string>(_config)];
        }

        var subreddits = subredditsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (subreddits.Length <= maxTasks)
        {
            // One task per subreddit
            return subreddits.Select(sub =>
            {
                var taskConfig = new Dictionary<string, string>(_config)
                {
                    [RedditConnectorConfig.SubredditsConfig] = sub
                };
                return taskConfig;
            }).ToList();
        }

        // Distribute subreddits across tasks
        var taskConfigs = new List<IDictionary<string, string>>();
        var subsPerTask = (int)Math.Ceiling((double)subreddits.Length / maxTasks);

        for (var i = 0; i < maxTasks; i++)
        {
            var taskSubs = subreddits.Skip(i * subsPerTask).Take(subsPerTask).ToArray();
            if (taskSubs.Length == 0) break;

            var taskConfig = new Dictionary<string, string>(_config)
            {
                [RedditConnectorConfig.SubredditsConfig] = string.Join(",", taskSubs)
            };
            taskConfigs.Add(taskConfig);
        }

        return taskConfigs;
    }
}
