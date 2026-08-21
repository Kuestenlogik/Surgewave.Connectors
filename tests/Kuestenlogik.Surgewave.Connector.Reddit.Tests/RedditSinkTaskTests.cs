using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Reddit.Tests;

/// <summary>
/// Covers the record-to-submission mapping the sink applies before it talks to Reddit.
/// </summary>
public class RedditSinkTaskTests
{
    [Fact]
    public void ParseRecord_TakesSubredditFromConfiguredJsonField()
    {
        using var task = new RedditSinkTask();
        task.Initialize(new TaskContext());
        task.Start(SinkConfig());

        var (subreddit, json) = task.ParseRecord(Record("""{"target":"csharp","title":"Hello"}"""));

        Assert.Equal("csharp", subreddit);
        Assert.NotNull(json);
    }

    [Fact]
    public void ParseRecord_KeepsDefaultSubreddit_WhenFieldIsAbsent()
    {
        using var task = new RedditSinkTask();
        task.Initialize(new TaskContext());
        task.Start(SinkConfig());

        var (subreddit, json) = task.ParseRecord(Record("""{"title":"Hello"}"""));

        Assert.Equal("surgewave", subreddit);
        Assert.NotNull(json);
    }

    [Fact]
    public void ParseRecord_KeepsDefaultSubreddit_ForNonJsonValues()
    {
        using var task = new RedditSinkTask();
        task.Initialize(new TaskContext());
        task.Start(SinkConfig());

        var (subreddit, json) = task.ParseRecord(Record("just some plain text"));

        Assert.Equal("surgewave", subreddit);
        Assert.Null(json);
    }

    [Fact]
    public void GetFieldValue_RendersScalarsAsText()
    {
        var json = Element("""{"title":"Hello","score":42,"nsfw":true,"tame":false}""");

        Assert.Equal("Hello", RedditSinkTask.GetFieldValue(json, "title"));
        Assert.Equal("42", RedditSinkTask.GetFieldValue(json, "score"));
        Assert.Equal("true", RedditSinkTask.GetFieldValue(json, "nsfw"));
        Assert.Equal("false", RedditSinkTask.GetFieldValue(json, "tame"));
        Assert.Null(RedditSinkTask.GetFieldValue(json, "absent"));
        Assert.Null(RedditSinkTask.GetFieldValue(json, null));
    }

    [Fact]
    public void GetBooleanField_AcceptsBooleansAndBooleanStrings()
    {
        var json = Element("""{"flag":true,"off":false,"text":"true","junk":"maybe","number":1}""");

        Assert.True(RedditSinkTask.GetBooleanField(json, "flag"));
        Assert.False(RedditSinkTask.GetBooleanField(json, "off"));
        Assert.True(RedditSinkTask.GetBooleanField(json, "text"));
        Assert.Null(RedditSinkTask.GetBooleanField(json, "junk"));
        Assert.Null(RedditSinkTask.GetBooleanField(json, "number"));
    }

    [Fact]
    public void GetDefaultTitle_PrefersTheRecordKey()
    {
        Assert.Equal("daily-thread", RedditSinkTask.GetDefaultTitle(Record("{}", key: "daily-thread")));
    }

    [Fact]
    public void GetDefaultTitle_FallsBackToTheTopic_WhenThereIsNoKey()
    {
        var title = RedditSinkTask.GetDefaultTitle(Record("{}"));

        Assert.Contains("posts", title, StringComparison.Ordinal);
    }

    private static JsonElement Element(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }

    private static SinkRecord Record(string value, string? key = null) => new()
    {
        Topic = "posts",
        Partition = 0,
        Offset = 7,
        Key = key is null ? null : Encoding.UTF8.GetBytes(key),
        Value = Encoding.UTF8.GetBytes(value),
        Timestamp = DateTimeOffset.UnixEpoch
    };

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [RedditConnectorConfig.TopicsConfig] = "posts",
        [RedditConnectorConfig.ClientIdConfig] = "client-id",
        [RedditConnectorConfig.ClientSecretConfig] = "client-secret",
        [RedditConnectorConfig.UsernameConfig] = "spez",
        [RedditConnectorConfig.PasswordConfig] = "hunter2",
        [RedditConnectorConfig.UserAgentConfig] = "Surgewave/1.0",
        [RedditConnectorConfig.DefaultSubredditConfig] = "surgewave",
        [RedditConnectorConfig.SubredditFieldConfig] = "target",
        [RedditConnectorConfig.TitleFieldConfig] = "title",
        [RedditConnectorConfig.TextFieldConfig] = "body"
    };
}
