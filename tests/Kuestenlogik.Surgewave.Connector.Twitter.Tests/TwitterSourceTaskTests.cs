using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Twitter.Tests;

/// <summary>
/// Tests for <see cref="TwitterSourceTask"/> driven through a stub transport. The interesting
/// behaviour is the request the task builds: without a <c>since_id</c> the API hands back the same
/// newest tweets on every poll, so the connector republishes them for as long as it runs.
/// </summary>
public class TwitterSourceTaskTests
{
    private const string BearerToken = "test-bearer";
    private const string Topic = "tweets";
    private const string EmptyPage = """{"data":[]}""";

    [Fact]
    public async Task PollAsync_InSearchMode_CallsRecentSearchWithAnEscapedQuery()
    {
        using var handler = new StubHandler();
        handler.RespondWith(HttpStatusCode.OK, Page(Tweet("1750000000000000001")));
        using var task = StartTask(handler, null,
            (TwitterConnectorConfig.SearchQuery, "#surgewave"),
            (TwitterConnectorConfig.MaxResults, "25"));

        await task.PollAsync(CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Get, request.Method);
        Assert.StartsWith("https://api.twitter.com/2/tweets/search/recent?", request.Url, StringComparison.Ordinal);
        Assert.Contains("query=%23surgewave", request.Url, StringComparison.Ordinal);
        Assert.Contains("max_results=25", request.Url, StringComparison.Ordinal);
        Assert.DoesNotContain("since_id", request.Url, StringComparison.Ordinal);
        Assert.Equal("Bearer", request.Authorization!.Scheme);
        Assert.Equal(BearerToken, request.Authorization.Parameter);
    }

    [Fact]
    public async Task PollAsync_InSearchMode_OnlyAsksForTweetsNewerThanTheLastOneItSaw()
    {
        using var handler = new StubHandler();
        handler.RespondWith(HttpStatusCode.OK, Page(Tweet("10"), Tweet("12")));
        handler.RespondWith(HttpStatusCode.OK, EmptyPage);
        using var task = StartTask(handler, null, (TwitterConnectorConfig.SearchQuery, "surgewave"));

        await task.PollAsync(CancellationToken.None);
        var second = await task.PollAsync(CancellationToken.None);

        Assert.Empty(second);
        Assert.Contains("since_id=12", handler.Requests[1].Url, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_InUserTimelineMode_ReadsEachConfiguredTimeline()
    {
        using var handler = new StubHandler();
        handler.RespondWith(HttpStatusCode.OK, Page(Tweet("20")));
        using var task = StartTask(handler, null, (TwitterConnectorConfig.UserIds, "4711"));

        await task.PollAsync(CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.StartsWith("https://api.twitter.com/2/users/4711/tweets?", request.Url, StringComparison.Ordinal);
        Assert.DoesNotContain("since_id", request.Url, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_InUserTimelineMode_OnlyAsksForTweetsNewerThanTheLastOneItSaw()
    {
        // Without a since_id the timeline endpoint keeps returning the same newest tweets, so
        // every poll republishes them.
        using var handler = new StubHandler();
        handler.RespondWith(HttpStatusCode.OK, Page(Tweet("20"), Tweet("21")));
        handler.RespondWith(HttpStatusCode.OK, EmptyPage);
        using var task = StartTask(handler, null, (TwitterConnectorConfig.UserIds, "4711"));

        await task.PollAsync(CancellationToken.None);
        var second = await task.PollAsync(CancellationToken.None);

        Assert.Empty(second);
        Assert.Contains("since_id=21", handler.Requests[1].Url, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_WhenTheApiCallFails_SurfacesTheErrorInsteadOfPollingSilently()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler();
        handler.FailWith(new HttpRequestException("connection reset by peer"));
        using var task = StartTask(handler, errors.Add, (TwitterConnectorConfig.SearchQuery, "surgewave"));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Empty(records);
        Assert.IsType<HttpRequestException>(Assert.Single(errors), exactMatch: false);
    }

    [Fact]
    public async Task PollAsync_DropsRetweetsWhenTheyAreExcluded()
    {
        using var handler = new StubHandler();
        handler.RespondWith(HttpStatusCode.OK, Page(
            Tweet("10", text: "RT @someone: original"),
            Tweet("11", text: "an original thought")));
        using var task = StartTask(handler, null,
            (TwitterConnectorConfig.SearchQuery, "surgewave"),
            (TwitterConnectorConfig.IncludeRetweets, "false"));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal("an original thought", TextOf(Assert.Single(records)));
    }

    [Fact]
    public async Task PollAsync_DropsRepliesWhenTheyAreExcluded()
    {
        using var handler = new StubHandler();
        handler.RespondWith(HttpStatusCode.OK, Page(
            Tweet("10", inReplyToUserId: "555"),
            Tweet("11", text: "standalone")));
        using var task = StartTask(handler, null,
            (TwitterConnectorConfig.SearchQuery, "surgewave"),
            (TwitterConnectorConfig.IncludeReplies, "false"));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal("standalone", TextOf(Assert.Single(records)));
    }

    [Fact]
    public async Task PollAsync_MapsTheTweetOntoTheRecord()
    {
        using var handler = new StubHandler();
        handler.RespondWith(HttpStatusCode.OK, Page(Tweet("1750000000000000001", text: "hello")));
        using var task = StartTask(handler, null, (TwitterConnectorConfig.SearchQuery, "surgewave"));

        var record = Assert.Single(await task.PollAsync(CancellationToken.None));

        Assert.Equal(Topic, record.Topic);
        Assert.Equal("1750000000000000001", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("1750000000000000001", Encoding.UTF8.GetString(record.Headers!["twitter.tweet.id"]));
        Assert.Equal("99", Encoding.UTF8.GetString(record.Headers["twitter.author.id"]));
        Assert.Equal("99", record.SourcePartition["author_id"]);
        Assert.Equal("1750000000000000001", record.SourceOffset["tweet_id"]);
        Assert.Equal(new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), record.Timestamp);

        using var document = JsonDocument.Parse(record.Value);
        var payload = document.RootElement;
        Assert.Equal("hello", payload.GetProperty("text").GetString());
        Assert.Equal("99", payload.GetProperty("author_id").GetString());
        Assert.Equal(3, payload.GetProperty("like_count").GetInt32());
    }

    private static TwitterSourceTask StartTask(
        StubHandler handler,
        Action<Exception>? raiseError = null,
        params (string Key, string Value)[] settings)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TwitterConnectorConfig.Topic] = Topic,
            [TwitterConnectorConfig.BearerToken] = BearerToken,

            // Polling is driven by the test, not by wall-clock time.
            [TwitterConnectorConfig.PollIntervalMs] = "0"
        };

        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new TwitterSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = raiseError ?? (_ => { }) });
        task.Start(config);
        return task;
    }

    private static string Page(params string[] tweets) => $$"""{"data":[{{string.Join(",", tweets)}}]}""";

    private static string Tweet(string id, string text = "hello", string? inReplyToUserId = null)
    {
        var replyTo = inReplyToUserId == null ? "null" : "\"" + inReplyToUserId + "\"";

        // Three dollar signs so the nested JSON object can close with "}}" as plain text.
        return $$$"""
            {"id":"{{{id}}}","text":"{{{text}}}","author_id":"99","created_at":"2026-01-02T03:04:05.000Z","in_reply_to_user_id":{{{replyTo}}},"public_metrics":{"retweet_count":1,"reply_count":2,"like_count":3}}
            """;
    }

    private static string? TextOf(SourceRecord record)
    {
        using var document = JsonDocument.Parse(record.Value);
        return document.RootElement.GetProperty("text").GetString();
    }

    private sealed record CapturedRequest(HttpMethod Method, string Url, AuthenticationHeaderValue? Authorization);

    /// <summary>
    /// A transport that answers from a scripted queue and records what was requested.
    /// </summary>
    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Queue<Func<HttpResponseMessage>> _responses = new();

        public List<CapturedRequest> Requests { get; } = [];

        public void RespondWith(HttpStatusCode status, string body) =>
            _responses.Enqueue(() => new HttpResponseMessage(status)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            });

        public void FailWith(Exception error) => _responses.Enqueue(() => throw error);

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Requests.Add(new CapturedRequest(
                request.Method,
                request.RequestUri?.AbsoluteUri ?? string.Empty,
                request.Headers.Authorization));

            if (_responses.Count > 0)
            {
                return Task.FromResult(_responses.Dequeue()());
            }

            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(EmptyPage, Encoding.UTF8, "application/json")
            });
        }
    }
}
