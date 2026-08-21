using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Instagram.Tests;

/// <summary>
/// Publish semantics of the Instagram Graph API sink. The two-step container/publish flow must
/// report failures instead of swallowing them, so the worker can retry or DLQ the batch.
/// </summary>
public class InstagramSinkTaskTests
{
    private const string ValidPayload = """{"caption":"hello","image_url":"https://cdn.example.com/a.jpg"}""";

    [Fact]
    public async Task PutAsync_CreatesMediaContainerThenPublishesIt()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, """{"id":"container-1"}""");
        handler.Enqueue(HttpStatusCode.OK, """{"id":"media-1"}""");

        using var task = new InstagramSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record(ValidPayload)], cts.Token);

        Assert.Equal(2, handler.Requests.Count);
        Assert.Equal("https://graph.facebook.com/v19.0/17841400000000000/media", handler.Requests[0].Url);
        Assert.Contains("caption=hello", handler.Requests[0].Body, StringComparison.Ordinal);
        Assert.Contains("image_url=https%3A%2F%2Fcdn.example.com%2Fa.jpg", handler.Requests[0].Body, StringComparison.Ordinal);
        Assert.Equal("https://graph.facebook.com/v19.0/17841400000000000/media_publish", handler.Requests[1].Url);
        Assert.Contains("creation_id=container-1", handler.Requests[1].Body, StringComparison.Ordinal);
        Assert.Contains("access_token=token-123", handler.Requests[1].Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_ThrowsAndSkipsPublish_WhenContainerCreationFails()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.InternalServerError, "");

        using var task = new InstagramSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync([Record(ValidPayload, offset: 42)], cts.Token));

        Assert.Contains("offset 42", error.Message, StringComparison.Ordinal);
        Assert.Contains("media container creation returned 500", error.Message, StringComparison.Ordinal);
        Assert.Single(handler.Requests);
        Assert.Same(error, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_Throws_WhenPublishStepFails()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, """{"id":"container-1"}""");
        handler.Enqueue(HttpStatusCode.BadRequest, """{"error":{"message":"nope"}}""");

        using var task = new InstagramSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync([Record(ValidPayload)], cts.Token));

        Assert.Contains("media publish returned 400", error.Message, StringComparison.Ordinal);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task PutAsync_Throws_WhenContainerResponseCarriesNoId()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, "{}");

        using var task = new InstagramSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync([Record(ValidPayload)], cts.Token));

        Assert.Contains("did not contain an id", error.Message, StringComparison.Ordinal);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_SkipsRecordWithoutImageUrl_AndReportsIt()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler();

        using var task = new InstagramSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"caption":"no image here"}""", offset: 7)], cts.Token);

        Assert.Empty(handler.Requests);
        Assert.Contains("image_url", Assert.Single(errors).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_SkipsUnparseableRecord_AndReportsIt()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler();

        using var task = new InstagramSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("not-json")], cts.Token);

        Assert.Empty(handler.Requests);
        Assert.Contains("not valid JSON", Assert.Single(errors).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_ReadsCaptionAndImageFromConfiguredFields()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.OK, """{"id":"container-9"}""");
        handler.Enqueue(HttpStatusCode.OK, """{"id":"media-9"}""");

        var config = SinkConfig();
        config[InstagramConnectorConfig.CaptionField] = "text";
        config[InstagramConnectorConfig.ImageUrlField] = "photo";

        using var task = new InstagramSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"text":"custom","photo":"https://cdn.example.com/b.jpg"}""")], cts.Token);

        Assert.Contains("caption=custom", handler.Requests[0].Body, StringComparison.Ordinal);
        Assert.Contains("image_url=https%3A%2F%2Fcdn.example.com%2Fb.jpg", handler.Requests[0].Body, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_RejectsMediaTypesThatAreNotImplemented()
    {
        using var task = new InstagramSinkTask();
        var config = SinkConfig();
        config[InstagramConnectorConfig.MediaType] = "video";

        var error = Assert.Throws<ArgumentException>(() => task.Start(config));

        Assert.Contains(InstagramConnectorConfig.MediaType, error.Message, StringComparison.Ordinal);
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [InstagramConnectorConfig.AccessToken] = "token-123",
        [InstagramConnectorConfig.BusinessAccountId] = "17841400000000000",
        [InstagramConnectorConfig.ApiVersion] = "v19.0"
    };

    private static SinkRecord Record(string json, long offset = 0) => new()
    {
        Topic = "instagram-posts",
        Partition = 0,
        Offset = offset,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Queue<HttpResponseMessage> _responses = new();

        public List<CapturedRequest> Requests { get; } = [];

        public void Enqueue(HttpStatusCode status, string body) =>
            _responses.Enqueue(new HttpResponseMessage(status)
            {
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            });

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var body = request.Content is null
                ? string.Empty
                : await request.Content.ReadAsStringAsync(cancellationToken);

            Requests.Add(new CapturedRequest(request.RequestUri?.ToString() ?? string.Empty, body));

            return _responses.Count > 0
                ? _responses.Dequeue()
                : new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("{}", Encoding.UTF8, "application/json")
                };
        }
    }

    private sealed record CapturedRequest(string Url, string Body);
}
