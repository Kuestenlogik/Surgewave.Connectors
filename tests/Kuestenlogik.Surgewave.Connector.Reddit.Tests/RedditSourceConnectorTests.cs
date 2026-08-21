namespace Kuestenlogik.Surgewave.Connector.Reddit.Tests;

/// <summary>
/// Covers required-configuration validation and how subreddits are spread over tasks.
/// </summary>
public class RedditSourceConnectorTests
{
    [Theory]
    [InlineData(RedditConnectorConfig.TopicConfig)]
    [InlineData(RedditConnectorConfig.ClientIdConfig)]
    [InlineData(RedditConnectorConfig.ClientSecretConfig)]
    [InlineData(RedditConnectorConfig.UsernameConfig)]
    [InlineData(RedditConnectorConfig.PasswordConfig)]
    [InlineData(RedditConnectorConfig.UserAgentConfig)]
    [InlineData(RedditConnectorConfig.SubredditsConfig)]
    public void Start_WithoutRequiredKey_Throws(string missingKey)
    {
        using var connector = new RedditSourceConnector();
        var config = SourceConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));

        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithBlankUserAgent_Throws()
    {
        // The user agent is what Reddit uses to identify the client, so an empty string is as
        // unusable as a missing key.
        using var connector = new RedditSourceConnector();
        var config = SourceConfig();
        config[RedditConnectorConfig.UserAgentConfig] = "   ";

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_GivesEverySubredditItsOwnTask_WhenTasksAllow()
    {
        using var connector = new RedditSourceConnector();
        var config = SourceConfig();
        config[RedditConnectorConfig.SubredditsConfig] = "dotnet, csharp ,programming";
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(5);

        Assert.Equal(3, taskConfigs.Count);
        Assert.Equal(
            new[] { "dotnet", "csharp", "programming" },
            taskConfigs.Select(c => c[RedditConnectorConfig.SubredditsConfig]));
    }

    [Fact]
    public void TaskConfigs_SpreadsSubredditsOverTheAvailableTasks()
    {
        using var connector = new RedditSourceConnector();
        var config = SourceConfig();
        config[RedditConnectorConfig.SubredditsConfig] = "a,b,c,d,e";
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(2);

        Assert.Equal(2, taskConfigs.Count);
        Assert.Equal("a,b,c", taskConfigs[0][RedditConnectorConfig.SubredditsConfig]);
        Assert.Equal("d,e", taskConfigs[1][RedditConnectorConfig.SubredditsConfig]);
    }

    [Fact]
    public void TaskConfigs_CarriesTheCredentialsIntoEveryTask()
    {
        using var connector = new RedditSourceConnector();
        var config = SourceConfig();
        config[RedditConnectorConfig.SubredditsConfig] = "dotnet,csharp";
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(2);

        Assert.All(taskConfigs, c =>
        {
            Assert.Equal("client-id", c[RedditConnectorConfig.ClientIdConfig]);
            Assert.Equal("Surgewave/1.0", c[RedditConnectorConfig.UserAgentConfig]);
            Assert.Equal("reddit-events", c[RedditConnectorConfig.TopicConfig]);
        });
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [RedditConnectorConfig.TopicConfig] = "reddit-events",
        [RedditConnectorConfig.ClientIdConfig] = "client-id",
        [RedditConnectorConfig.ClientSecretConfig] = "client-secret",
        [RedditConnectorConfig.UsernameConfig] = "spez",
        [RedditConnectorConfig.PasswordConfig] = "hunter2",
        [RedditConnectorConfig.UserAgentConfig] = "Surgewave/1.0",
        [RedditConnectorConfig.SubredditsConfig] = "dotnet"
    };
}
