using System.Net;
using System.Text;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Http;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Google.Photos.Tests;

/// <summary>
/// Exercises the upload side against a stubbed Google Photos upload endpoint: what the raw
/// upload request looks like and what happens when it is rejected.
/// </summary>
public class GooglePhotosSinkTaskTests
{
    [Theory]
    [InlineData("holiday.jpg", "image/jpeg")]
    [InlineData("holiday.JPEG", "image/jpeg")]
    [InlineData("holiday.png", "image/png")]
    [InlineData("holiday.webp", "image/webp")]
    [InlineData("clip.mp4", "video/mp4")]
    [InlineData("clip.MOV", "video/quicktime")]
    public void GetMimeType_MapsKnownExtensionsRegardlessOfCase(string filename, string expected)
    {
        Assert.Equal(expected, GooglePhotosSinkTask.GetMimeType(filename));
    }

    [Theory]
    [InlineData("notes.txt")]
    [InlineData("noextension")]
    public void GetMimeType_ForAnythingElse_IsOctetStream(string filename)
    {
        Assert.Equal("application/octet-stream", GooglePhotosSinkTask.GetMimeType(filename));
    }

    [Fact]
    public async Task UploadBytesAsync_PostsTheRawBytesWithTheBearerTokenAndUploadHeaders()
    {
        using var handler = new RecordingHandler(_ => Text(HttpStatusCode.OK, "upload-token-1"));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new GooglePhotosSinkTask(new StubCredential("test-token"), http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        await task.UploadBytesAsync([7, 8, 9], "clip.mp4", TestContext.Current.CancellationToken);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("https://photoslibrary.googleapis.com/v1/uploads", request.Url);
        Assert.Equal("Bearer test-token", request.Authorization);
        Assert.Equal("video/mp4", request.UploadContentType);
        Assert.Equal("raw", request.UploadProtocol);
        Assert.Equal("application/octet-stream", request.ContentType);
        Assert.Equal(new byte[] { 7, 8, 9 }, request.Body);
    }

    [Fact]
    public async Task UploadBytesAsync_ReturnsTheUploadTokenFromTheResponseBody()
    {
        using var handler = new RecordingHandler(_ => Text(HttpStatusCode.OK, "upload-token-1"));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new GooglePhotosSinkTask(new StubCredential("test-token"), http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var token = await task.UploadBytesAsync([1], "holiday.jpg", TestContext.Current.CancellationToken);

        Assert.Equal("upload-token-1", token);
    }

    [Fact]
    public async Task UploadBytesAsync_WhenTheUploadIsRejected_RaisesAndThrowsInsteadOfDroppingTheRecord()
    {
        // The upload used to swallow every failure and return null, and PutAsync then skipped
        // the record: a transient 5xx silently lost the photo with no retry and no error.
        var errors = new List<Exception>();
        using var handler = new RecordingHandler(_ => Text(HttpStatusCode.InternalServerError, "nope"));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new GooglePhotosSinkTask(new StubCredential("test-token"), http);
        task.Initialize(new TaskContext { RaiseError = errors.Add });

        var thrown = await Assert.ThrowsAnyAsync<HttpRequestException>(() =>
            task.UploadBytesAsync([1], "holiday.jpg", TestContext.Current.CancellationToken));

        Assert.Same(thrown, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_WithNothingToUpload_NeverCallsTheApi()
    {
        using var handler = new RecordingHandler(_ => Text(HttpStatusCode.OK, "upload-token-1"));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = new GooglePhotosSinkTask(new StubCredential("test-token"), http);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        await task.PutAsync([RecordWithBytes([]), RecordWithoutValue()], TestContext.Current.CancellationToken);

        Assert.Empty(handler.Requests);
    }

    private static HttpResponseMessage Text(HttpStatusCode status, string body) =>
        new(status) { Content = new StringContent(body, Encoding.UTF8, "text/plain") };

    private static SinkRecord RecordWithBytes(byte[] value) => new()
    {
        Topic = "photo-uploads",
        Partition = 0,
        Offset = 0,
        Value = value
    };

    private static SinkRecord RecordWithoutValue() => new()
    {
        Topic = "photo-uploads",
        Partition = 0,
        Offset = 0,
        Value = null!
    };

    /// <summary>Hands out a fixed access token; the upload path only needs that much of a credential.</summary>
    private sealed class StubCredential(string accessToken) : ICredential
    {
        public Task<string> GetAccessTokenForRequestAsync(
            string? authUri = null,
            CancellationToken cancellationToken = default) => Task.FromResult(accessToken);

        public void Initialize(ConfigurableHttpClient httpClient)
        {
        }
    }

    private sealed record CapturedRequest(
        HttpMethod Method,
        string Url,
        string? Authorization,
        string? UploadContentType,
        string? UploadProtocol,
        string? ContentType,
        byte[] Body);

    /// <summary>Answers every upload from a canned responder and records what was sent.</summary>
    private sealed class RecordingHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public List<CapturedRequest> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(new CapturedRequest(
                request.Method,
                request.RequestUri!.ToString(),
                request.Headers.Authorization?.ToString(),
                HeaderOrNull(request, "X-Goog-Upload-Content-Type"),
                HeaderOrNull(request, "X-Goog-Upload-Protocol"),
                request.Content?.Headers.ContentType?.MediaType,
                request.Content == null
                    ? []
                    : await request.Content.ReadAsByteArrayAsync(cancellationToken)));

            return respond(request);
        }

        private static string? HeaderOrNull(HttpRequestMessage request, string name) =>
            request.Headers.TryGetValues(name, out var values) ? string.Join(",", values) : null;
    }
}
