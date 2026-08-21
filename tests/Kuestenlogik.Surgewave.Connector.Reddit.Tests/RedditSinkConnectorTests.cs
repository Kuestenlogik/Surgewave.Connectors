namespace Kuestenlogik.Surgewave.Connector.Reddit.Tests;

/// <summary>
/// Covers the posting-target validation the sink connector performs before tasks are started.
/// </summary>
public class RedditSinkConnectorTests
{
    [Fact]
    public void Start_WithoutSubredditTarget_Throws()
    {
        using var connector = new RedditSinkConnector();
        var config = SinkConfig();
        config.Remove(RedditConnectorConfig.DefaultSubredditConfig);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(RedditConnectorConfig.DefaultSubredditConfig, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithSubredditField_NeedsNoDefaultSubreddit()
    {
        using var connector = new RedditSinkConnector();
        var config = SinkConfig();
        config.Remove(RedditConnectorConfig.DefaultSubredditConfig);
        config[RedditConnectorConfig.SubredditFieldConfig] = "target";

        connector.Start(config);

        Assert.Single(connector.TaskConfigs(1));
    }

    [Fact]
    public void Start_InCommentMode_NeedsNoSubredditTarget()
    {
        // Replies go to a parent id, not to a subreddit, so the subreddit requirement is lifted.
        using var connector = new RedditSinkConnector();
        var config = SinkConfig();
        config.Remove(RedditConnectorConfig.DefaultSubredditConfig);
        config[RedditConnectorConfig.ReplyToCommentsConfig] = "true";

        connector.Start(config);

        Assert.Single(connector.TaskConfigs(4));
    }

    [Theory]
    [InlineData(RedditConnectorConfig.TopicsConfig)]
    [InlineData(RedditConnectorConfig.ClientIdConfig)]
    [InlineData(RedditConnectorConfig.UserAgentConfig)]
    public void Start_WithoutRequiredKey_Throws(string missingKey)
    {
        using var connector = new RedditSinkConnector();
        var config = SinkConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [RedditConnectorConfig.TopicsConfig] = "posts",
        [RedditConnectorConfig.ClientIdConfig] = "client-id",
        [RedditConnectorConfig.ClientSecretConfig] = "client-secret",
        [RedditConnectorConfig.UsernameConfig] = "spez",
        [RedditConnectorConfig.PasswordConfig] = "hunter2",
        [RedditConnectorConfig.UserAgentConfig] = "Surgewave/1.0",
        [RedditConnectorConfig.DefaultSubredditConfig] = "surgewave"
    };
}
