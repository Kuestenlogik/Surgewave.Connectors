using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Reddit.Tests;

/// <summary>
/// Reddit.NET cannot fetch its own OAuth token, so the task builds its client lazily on the first
/// poll that actually has work. These tests keep <c>Start</c> free of network calls and the
/// no-work path free of a pointless token exchange - both are offline-observable: any real call
/// to reddit.com would fail here.
/// </summary>
public class RedditSourceTaskTests
{
    [Fact]
    public void Start_DoesNotAuthenticateEagerly()
    {
        using var task = new RedditSourceTask();
        task.Initialize(new TaskContext());

        task.Start(SourceConfig());

        // Nothing has been emitted yet, so there is no checkpoint to report either.
        Assert.Null(task.CurrentOffset);

        task.Stop();
    }

    [Fact]
    public async Task PollAsync_WithoutAssignedSubreddits_ReturnsNoRecords()
    {
        // An idle task must not exchange credentials for a token it has no use for.
        using var task = new RedditSourceTask();
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config.Remove(RedditConnectorConfig.SubredditsConfig);
        task.Start(config);

        Assert.Empty(await task.PollAsync(CancellationToken.None));
    }

    [Fact]
    public void Start_WithoutTheUserAgent_Throws()
    {
        // Reddit's API rules require a distinct user agent, so the task cannot make one up.
        using var task = new RedditSourceTask();
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config.Remove(RedditConnectorConfig.UserAgentConfig);

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [RedditConnectorConfig.TopicConfig] = "reddit-events",
        [RedditConnectorConfig.ClientIdConfig] = "client-id",
        [RedditConnectorConfig.ClientSecretConfig] = "client-secret",
        [RedditConnectorConfig.UsernameConfig] = "spez",
        [RedditConnectorConfig.PasswordConfig] = "hunter2",
        [RedditConnectorConfig.UserAgentConfig] = "Surgewave/1.0 by spez",
        [RedditConnectorConfig.SubredditsConfig] = "dotnet"
    };
}
