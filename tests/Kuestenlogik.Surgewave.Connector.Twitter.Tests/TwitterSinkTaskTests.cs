using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Twitter.Tests;

/// <summary>
/// Tests for <see cref="TwitterSinkTask"/> driven through a stub transport. The response of
/// <c>POST /2/tweets</c> has to be checked: if a 401 or a rate-limit answer is treated as success
/// the worker commits the offset and the tweet is lost for good.
/// </summary>
public class TwitterSinkTaskTests
{
    private const string TweetsEndpoint = "https://api.twitter.com/2/tweets";

    [Fact]
    public async Task PutAsync_PostsTheTweetTextToTheApi()
    {
        using var handler = new StubHandler();
        using var task = StartTask(handler);

        await task.PutAsync([Record("""{"text":"hello from surgewave"}""")], CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal(TweetsEndpoint, request.Url);

        using var document = JsonDocument.Parse(request.Body);
        Assert.Equal("hello from surgewave", document.RootElement.GetProperty("text").GetString());
    }

    [Fact]
    public async Task PutAsync_SignsTheRequestWithOAuth()
    {
        using var handler = new StubHandler();
        using var task = StartTask(handler);

        await task.PutAsync([Record("""{"text":"hello"}""")], CancellationToken.None);

        var authorization = Assert.Single(handler.Requests).Authorization;
        Assert.NotNull(authorization);
        Assert.Equal("OAuth", authorization.Scheme);
        Assert.Contains("oauth_consumer_key=\"consumer-key\"", authorization.Parameter!, StringComparison.Ordinal);
        Assert.Contains("oauth_signature=", authorization.Parameter!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_WithAReplyField_AttachesTheReplyReference()
    {
        using var handler = new StubHandler();
        using var task = StartTask(handler, null, (TwitterConnectorConfig.ReplyToField, "reply_to"));

        await task.PutAsync(
            [Record("""{"text":"answering","reply_to":"1750000000000000001"}""")],
            CancellationToken.None);

        using var document = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        var reply = document.RootElement.GetProperty("reply");
        Assert.Equal("1750000000000000001", reply.GetProperty("in_reply_to_tweet_id").GetString());
    }

    [Fact]
    public async Task PutAsync_UsesTheConfiguredTextField()
    {
        using var handler = new StubHandler();
        using var task = StartTask(handler, null, (TwitterConnectorConfig.TextField, "body"));

        await task.PutAsync([Record("""{"body":"from a custom field"}""")], CancellationToken.None);

        using var document = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        Assert.Equal("from a custom field", document.RootElement.GetProperty("text").GetString());
    }

    [Fact]
    public async Task PutAsync_WhenTwitterRejectsTheTweet_ThrowsSoTheWorkerCanRetry()
    {
        // Returning normally here would let the worker commit the offset for a tweet that
        // Twitter never accepted.
        var errors = new List<Exception>();
        using var handler = new StubHandler();
        handler.RespondWith(HttpStatusCode.Unauthorized, """{"title":"Unauthorized"}""");
        using var task = StartTask(handler, errors.Add);

        var error = await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"text":"hello"}""")], CancellationToken.None));

        Assert.Contains("401", error.Message, StringComparison.Ordinal);
        Assert.Same(error, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_WhenTwitterRateLimitsTheTweet_Throws()
    {
        using var handler = new StubHandler();
        handler.RespondWith(HttpStatusCode.TooManyRequests, """{"title":"Too Many Requests"}""");
        using var task = StartTask(handler);

        var error = await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"text":"hello"}""")], CancellationToken.None));

        Assert.Contains("429", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_WithAPoisonRecord_SurfacesItAndKeepsGoing()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler();
        using var task = StartTask(handler, errors.Add);

        await task.PutAsync(
            [Record("this is not json"), Record("""{"text":"still delivered"}""")],
            CancellationToken.None);

        Assert.Single(errors);

        using var document = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        Assert.Equal("still delivered", document.RootElement.GetProperty("text").GetString());
    }

    [Fact]
    public async Task PutAsync_SkipsARecordWithoutAnyText()
    {
        using var handler = new StubHandler();
        using var task = StartTask(handler);

        await task.PutAsync([Record("""{"text":""}""")], CancellationToken.None);

        Assert.Empty(handler.Requests);
    }

    private static TwitterSinkTask StartTask(
        StubHandler handler,
        Action<Exception>? raiseError = null,
        params (string Key, string Value)[] settings)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [TwitterConnectorConfig.ConsumerKey] = "consumer-key",
            [TwitterConnectorConfig.ConsumerSecret] = "consumer-secret",
            [TwitterConnectorConfig.AccessToken] = "access-token",
            [TwitterConnectorConfig.AccessTokenSecret] = "access-token-secret"
        };

        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new TwitterSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = raiseError ?? (_ => { }) });
        task.Start(config);
        return task;
    }

    private static SinkRecord Record(string json) => new()
    {
        Topic = "outbound",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private sealed record CapturedRequest(HttpMethod Method, string Url, AuthenticationHeaderValue? Authorization, string Body);

    /// <summary>
    /// A transport that answers from a scripted queue and records what was sent.
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

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var body = request.Content == null
                ? string.Empty
                : await request.Content.ReadAsStringAsync(cancellationToken);

            Requests.Add(new CapturedRequest(
                request.Method,
                request.RequestUri?.AbsoluteUri ?? string.Empty,
                request.Headers.Authorization,
                body));

            if (_responses.Count > 0)
            {
                return _responses.Dequeue()();
            }

            return new HttpResponseMessage(HttpStatusCode.Created)
            {
                Content = new StringContent("""{"data":{"id":"1750000000000000002"}}""", Encoding.UTF8, "application/json")
            };
        }
    }
}
