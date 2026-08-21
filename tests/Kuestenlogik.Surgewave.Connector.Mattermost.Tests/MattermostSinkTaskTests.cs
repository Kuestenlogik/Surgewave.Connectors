using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mattermost.Tests;

/// <summary>
/// Covers the request the sink builds and the failure path: a post Mattermost rejects has to
/// fail the batch so the worker retries or dead-letters it, instead of committing an offset for
/// a message that was never delivered.
/// </summary>
public class MattermostSinkTaskTests
{
    [Fact]
    public async Task PutAsync_PostsTheMessageToTheConfiguredChannel()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.Created, """{"id":"post-1"}"""));
        using var http = Client(handler);
        using var task = new MattermostSinkTask(http);
        task.Initialize(new TaskContext());
        task.Start(SinkConfig());

        await task.PutAsync([Record("""{"message":"hello"}""")], CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("http://mattermost.test/api/v4/posts", request.Uri);
        Assert.Equal("Bearer token-1", request.Authorization);
        Assert.Equal("application/json", request.ContentType);
        Assert.Equal("channel-1", Field(request.Body, "channel_id"));
        Assert.Equal("hello", Field(request.Body, "message"));
    }

    [Fact]
    public async Task PutAsync_ThrowsWhenMattermostRejectsThePost()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.Unauthorized, """{"message":"Invalid or expired session"}"""));
        using var http = Client(handler);
        using var task = new MattermostSinkTask(http);
        task.Initialize(new TaskContext());
        task.Start(SinkConfig());

        // The worker has to see the failure so it can retry or dead-letter the record instead of
        // committing an offset for a message Mattermost never accepted.
        await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"message":"hello"}""")], CancellationToken.None));
    }

    [Fact]
    public async Task PutAsync_StopsAtTheFirstRejectedPost()
    {
        var call = 0;
        using var handler = new StubHandler(_ => ++call == 1
            ? Json(HttpStatusCode.Created, """{"id":"post-1"}""")
            : Json(HttpStatusCode.InternalServerError, "{}"));
        using var http = Client(handler);
        using var task = new MattermostSinkTask(http);
        task.Initialize(new TaskContext());
        task.Start(SinkConfig());

        await Assert.ThrowsAsync<HttpRequestException>(() => task.PutAsync(
            [
                Record("""{"message":"first"}"""),
                Record("""{"message":"second"}"""),
                Record("""{"message":"third"}""")
            ],
            CancellationToken.None));

        // The third record must not be posted after the second one failed - the whole batch is
        // replayed by the worker.
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task PutAsync_ReadsTheConfiguredMessageField()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.Created, """{"id":"post-1"}"""));
        using var http = Client(handler);
        using var task = new MattermostSinkTask(http);
        task.Initialize(new TaskContext());

        var config = SinkConfig();
        config[MattermostConnectorConfig.MessageField] = "text";
        task.Start(config);

        await task.PutAsync([Record("""{"text":"from the text field","message":"ignored"}""")], CancellationToken.None);

        Assert.Equal("from the text field", Field(Assert.Single(handler.Requests).Body, "message"));
    }

    [Fact]
    public async Task PutAsync_SendsTheWholePayload_WhenTheMessageFieldIsAbsent()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.Created, """{"id":"post-1"}"""));
        using var http = Client(handler);
        using var task = new MattermostSinkTask(http);
        task.Initialize(new TaskContext());
        task.Start(SinkConfig());

        const string value = """{"body":"payload without a message field"}""";
        await task.PutAsync([Record(value)], CancellationToken.None);

        Assert.Equal(value, Field(Assert.Single(handler.Requests).Body, "message"));
    }

    [Fact]
    public async Task PutAsync_SkipsRecordsWithoutAMessage()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.Created, """{"id":"post-1"}"""));
        using var http = Client(handler);
        using var task = new MattermostSinkTask(http);
        task.Initialize(new TaskContext());
        task.Start(SinkConfig());

        await task.PutAsync(
            [
                Record("""{"message":"   "}"""),
                Record(string.Empty)
            ],
            CancellationToken.None);

        Assert.Empty(handler.Requests);
    }

    private static Dictionary<string, string> SinkConfig() => new(StringComparer.Ordinal)
    {
        [MattermostConnectorConfig.ServerUrl] = "http://mattermost.test",
        [MattermostConnectorConfig.AccessToken] = "token-1",
        [MattermostConnectorConfig.ChannelId] = "channel-1"
    };

    private static HttpClient Client(StubHandler handler)
        => new(handler) { BaseAddress = new Uri("http://mattermost.test") };

    private static SinkRecord Record(string value) => new()
    {
        Topic = "mattermost-out",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(value),
        Timestamp = DateTimeOffset.UnixEpoch
    };

    private static string? Field(string body, string name)
    {
        using var document = JsonDocument.Parse(body);
        return document.RootElement.GetProperty(name).GetString();
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string json)
        => new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private sealed class CapturedRequest
    {
        public required HttpMethod Method { get; init; }

        public required string Uri { get; init; }

        public required string Body { get; init; }

        public required string? ContentType { get; init; }

        public required string? Authorization { get; init; }
    }

    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> responder) : HttpMessageHandler
    {
        public List<CapturedRequest> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(new CapturedRequest
            {
                Method = request.Method,
                Uri = request.RequestUri?.ToString() ?? string.Empty,
                Body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken),
                ContentType = request.Content?.Headers.ContentType?.MediaType,
                Authorization = request.Headers.Authorization?.ToString()
            });

            return responder(request);
        }
    }
}
