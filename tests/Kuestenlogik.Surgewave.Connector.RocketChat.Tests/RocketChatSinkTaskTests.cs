using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RocketChat.Tests;

/// <summary>
/// The sink used to run every post inside an empty catch and never looked at the response status,
/// so a rejected message was dropped while its offset was committed. These tests drive the task
/// through a stubbed transport and keep both the request shape and the failure path honest.
/// </summary>
public class RocketChatSinkTaskTests
{
    [Fact]
    public async Task PutAsync_PostsTheMessageWithTheAuthenticationHeaders()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, """{"success":true}"""));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        await task.PutAsync([Record("""{"text":"hello"}""")], CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("http://rocket.test/api/v1/chat.postMessage", request.Uri);
        Assert.Equal("token-1", request.Headers["X-Auth-Token"]);
        Assert.Equal("user-1", request.Headers["X-User-Id"]);
        Assert.Equal("application/json", request.ContentType);
        Assert.Equal("general", Field(request.Body, "roomId"));
        Assert.Equal("hello", Field(request.Body, "text"));
    }

    [Fact]
    public async Task PutAsync_RoutesByRecordRoomId_AndFallsBackToTheDefaultRoom()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, """{"success":true}"""));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = SinkConfig();
        config[RocketChatConnectorConfig.RoomIdField] = "room";
        task.Start(config);

        await task.PutAsync(
            [
                Record("""{"room":"ops","text":"deploy done"}"""),
                Record("""{"text":"no room here"}""")
            ],
            CancellationToken.None);

        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal("ops", Field(handler.Requests[0].Body, "roomId"));
        Assert.Equal("general", Field(handler.Requests[1].Body, "roomId"));
    }

    [Fact]
    public async Task PutAsync_SurfacesRejectedPosts_InsteadOfDroppingThem()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.Unauthorized, """{"success":false}"""));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSinkTask(http);

        Exception? raised = null;
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });
        task.Start(SinkConfig());

        var thrown = await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"text":"hello"}""")], CancellationToken.None));

        // The worker has to see the failure so it can retry or dead-letter the record instead of
        // committing an offset for a message Rocket.Chat never accepted.
        Assert.Same(thrown, raised);
    }

    [Fact]
    public async Task PutAsync_SendsTheWholeValue_WhenTheTextFieldIsAbsent()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.OK, """{"success":true}"""));
        using var http = new HttpClient(handler) { BaseAddress = new Uri("http://rocket.test") };
        using var task = new RocketChatSinkTask(http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        const string value = """{"body":"payload without a text field"}""";
        await task.PutAsync([Record(value)], CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(value, Field(request.Body, "text"));
    }

    [Fact]
    public void Start_WithoutAuthToken_Throws()
    {
        using var task = new RocketChatSinkTask();
        task.Initialize(new TaskContext());

        var config = SinkConfig();
        config.Remove(RocketChatConnectorConfig.AuthToken);

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [RocketChatConnectorConfig.ServerUrl] = "http://rocket.test",
        [RocketChatConnectorConfig.UserId] = "user-1",
        [RocketChatConnectorConfig.AuthToken] = "token-1",
        [RocketChatConnectorConfig.DefaultRoomId] = "general"
    };

    private static SinkRecord Record(string value) => new()
    {
        Topic = "chat-out",
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

        public required IReadOnlyDictionary<string, string> Headers { get; init; }
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
                Headers = request.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value), StringComparer.OrdinalIgnoreCase)
            });

            return responder(request);
        }
    }
}
