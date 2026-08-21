using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Facebook.Tests;

/// <summary>
/// Tests for <see cref="FacebookSinkTask"/> driven through a stub transport. Records are
/// acknowledged as soon as PutAsync returns, so anything the Graph API refuses has to either
/// fail the batch (retry/DLQ) or be reported as a poison record - never disappear quietly.
/// </summary>
public class FacebookSinkTaskTests
{
    private const string PageId = "1234567890";
    private const string AccessToken = "page-access-token";

    [Fact]
    public async Task PutAsync_PostsAFeedMessageToThePageFeed()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors);

        await task.PutAsync([Record("""{"message":"hello world"}""")], CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal($"https://graph.facebook.com/v18.0/{PageId}/feed", request.Url?.AbsoluteUri);

        var form = ParseForm(request.Body);
        Assert.Equal(AccessToken, form["access_token"]);
        Assert.Equal("hello world", form["message"]);
        Assert.Empty(errors);
    }

    [Fact]
    public async Task PutAsync_AttachesTheLinkWhenALinkFieldIsConfigured()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors, (FacebookConnectorConfig.LinkField, "link"));

        await task.PutAsync(
            [Record("""{"message":"read this","link":"https://example.invalid/post"}""")],
            CancellationToken.None);

        var form = ParseForm(Assert.Single(handler.Requests).Body);
        Assert.Equal("https://example.invalid/post", form["link"]);
    }

    [Fact]
    public async Task PutAsync_ThrowsWhenTheGraphApiRejectsThePost()
    {
        // A rejected post must fail the batch: the worker retries or DLQs it, whereas a
        // swallowed failure would acknowledge a post that never happened.
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.BadRequest, """{"error":{"message":"Invalid OAuth token"}}""");
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors);

        var ex = await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record("""{"message":"hello"}""")], CancellationToken.None));

        Assert.Contains("400", ex.Message, StringComparison.Ordinal);
        Assert.Contains("Invalid OAuth token", ex.Message, StringComparison.Ordinal);
        Assert.Same(ex, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_StopsTheBatchAtTheFirstRejectedPost()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.TooManyRequests, "rate limited");
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors);

        await Assert.ThrowsAsync<HttpRequestException>(() => task.PutAsync(
            [Record("""{"message":"first"}"""), Record("""{"message":"second"}""")],
            CancellationToken.None));

        // The second record must not be posted (and thereby acknowledged) after the first failed.
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_PhotoPost_SendsTheMediaUrlAndCaption()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors, (FacebookConnectorConfig.PostType, "photo"));

        await task.PutAsync(
            [Record("""{"message":"a caption","image_url":"https://cdn.invalid/pic.jpg"}""")],
            CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal($"https://graph.facebook.com/v18.0/{PageId}/photos", request.Url?.AbsoluteUri);

        var form = ParseForm(request.Body);
        Assert.Equal("https://cdn.invalid/pic.jpg", form["url"]);
        Assert.Equal("a caption", form["caption"]);
        Assert.False(form.ContainsKey("message"));
    }

    [Fact]
    public async Task PutAsync_VideoPost_SendsTheMediaUrlAsFileUrlAndDescription()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(
            handler,
            errors,
            (FacebookConnectorConfig.PostType, "video"),
            (FacebookConnectorConfig.ImageUrlField, "media_url"));

        await task.PutAsync(
            [Record("""{"message":"a description","media_url":"https://cdn.invalid/clip.mp4"}""")],
            CancellationToken.None);

        var request = Assert.Single(handler.Requests);
        Assert.Equal($"https://graph.facebook.com/v18.0/{PageId}/videos", request.Url?.AbsoluteUri);

        var form = ParseForm(request.Body);
        Assert.Equal("https://cdn.invalid/clip.mp4", form["file_url"]);
        Assert.Equal("a description", form["description"]);
    }

    [Fact]
    public async Task PutAsync_PhotoPostWithoutAMediaUrl_IsReportedInsteadOfPosted()
    {
        // The Graph API rejects /photos without a media source, so the record is poison -
        // report it and move on rather than send a request that cannot succeed.
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors, (FacebookConnectorConfig.PostType, "photo"));

        await task.PutAsync(
            [Record("""{"message":"no picture here"}"""), Record("""{"message":"ok","image_url":"https://cdn.invalid/p.jpg"}""")],
            CancellationToken.None);

        var error = Assert.Single(errors);
        Assert.IsType<InvalidOperationException>(error);
        Assert.Contains("image_url", error.Message, StringComparison.Ordinal);

        // The healthy record after it is still posted.
        var request = Assert.Single(handler.Requests);
        Assert.Equal($"https://graph.facebook.com/v18.0/{PageId}/photos", request.Url?.AbsoluteUri);
    }

    [Fact]
    public async Task PutAsync_MalformedJson_IsReportedAndTheBatchContinues()
    {
        using var handler = new StubHandler();
        var errors = new List<Exception>();
        using var task = StartTask(handler, errors);

        await task.PutAsync(
            [Record("this is not json"), Record("""{"message":"still delivered"}""")],
            CancellationToken.None);

        Assert.IsType<InvalidOperationException>(Assert.Single(errors));
        var form = ParseForm(Assert.Single(handler.Requests).Body);
        Assert.Equal("still delivered", form["message"]);
    }

    [Fact]
    public void Start_RejectsAPostTypeTheConnectorCannotSend()
    {
        using var handler = new StubHandler();
        using var task = new FacebookSinkTask(handler);

        var config = BaseConfig();
        config[FacebookConnectorConfig.PostType] = "story";

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(FacebookConnectorConfig.PostType, ex.Message, StringComparison.Ordinal);
    }

    private static FacebookSinkTask StartTask(
        StubHandler handler,
        List<Exception> errors,
        params (string Key, string Value)[] settings)
    {
        var config = BaseConfig();
        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new FacebookSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(config);
        return task;
    }

    private static Dictionary<string, string> BaseConfig() => new(StringComparer.Ordinal)
    {
        [FacebookConnectorConfig.AccessToken] = AccessToken,
        [FacebookConnectorConfig.PageId] = PageId
    };

    private static SinkRecord Record(string json) => new()
    {
        Topic = "posts",
        Partition = 0,
        Offset = 17,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private static Dictionary<string, string> ParseForm(string body)
    {
        var form = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in body.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = pair.Split('=', 2);
            var value = parts.Length == 2 ? Uri.UnescapeDataString(parts[1].Replace('+', ' ')) : string.Empty;
            form[Uri.UnescapeDataString(parts[0])] = value;
        }

        return form;
    }

    private sealed record RecordedRequest(HttpMethod Method, Uri? Url, string Body);

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Queue<(HttpStatusCode Status, string Body)> _responses = new();

        public List<RecordedRequest> Requests { get; } = [];

        public void EnqueueResponse(HttpStatusCode status, string body) => _responses.Enqueue((status, body));

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var body = request.Content == null
                ? string.Empty
                : await request.Content.ReadAsStringAsync(cancellationToken);

            Requests.Add(new RecordedRequest(request.Method, request.RequestUri, body));

            var response = _responses.Count > 0
                ? _responses.Dequeue()
                : (Status: HttpStatusCode.OK, Body: """{"id":"1234567890_1"}""");

            return new HttpResponseMessage(response.Status) { Content = new StringContent(response.Body) };
        }
    }
}
