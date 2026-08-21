using System.Net;
using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Connector.Flink.Tests;

/// <summary>
/// Tests for <see cref="FlinkRestClient"/> driven through a stub transport: the exact endpoint
/// each operation hits, how credentials travel, and how Flink's hyphenated JSON maps onto the
/// DTOs. A wrong URL or a mistyped query parameter silently manages the wrong job.
/// </summary>
public class FlinkRestClientTests
{
    private const string BaseUrl = "http://localhost:8081";

    [Fact]
    public async Task GetJobsAsync_ReadsTheJobsOverviewAndMapsHyphenatedFields()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.OK, """{"jobs":[{"jid":"job-1","name":"wordcount","state":"RUNNING","start-time":1700000000000,"tasks":{"total":4,"running":4}}]}""");
        using var client = new FlinkRestClient(handler, BaseUrl);

        var overview = await client.GetJobsAsync(CancellationToken.None);

        var recorded = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Get, recorded.Method);
        Assert.Equal("http://localhost:8081/jobs/overview", recorded.Url?.AbsoluteUri);

        var job = Assert.Single(overview.Jobs);
        Assert.Equal("job-1", job.Jid);
        Assert.Equal("RUNNING", job.State);
        Assert.Equal(1700000000000L, job.StartTime);
        Assert.Equal(4, Assert.IsType<JobTaskCounts>(job.Tasks).Total);
    }

    [Fact]
    public async Task BaseUrl_WithATrailingSlash_DoesNotProduceADoubleSlash()
    {
        using var handler = new StubHandler();
        using var client = new FlinkRestClient(handler, "http://localhost:8081/");

        await client.GetClusterOverviewAsync(CancellationToken.None);

        Assert.Equal("http://localhost:8081/overview", Assert.Single(handler.Requests).Url?.AbsoluteUri);
    }

    [Fact]
    public async Task BasicAuth_SendsTheCredentialsAsABasicHeader()
    {
        using var handler = new StubHandler();
        using var client = new FlinkRestClient(handler, BaseUrl, authType: "basic", username: "flink", password: "s3cret");

        await client.GetClusterOverviewAsync(CancellationToken.None);

        var expected = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("flink:s3cret"));
        Assert.Equal(expected, Assert.Single(handler.Requests).Authorization);
    }

    [Fact]
    public async Task BearerAuth_SendsTheTokenAsABearerHeader()
    {
        using var handler = new StubHandler();
        using var client = new FlinkRestClient(handler, BaseUrl, authType: "bearer", token: "token-123");

        await client.GetClusterOverviewAsync(CancellationToken.None);

        Assert.Equal("Bearer token-123", Assert.Single(handler.Requests).Authorization);
    }

    [Fact]
    public async Task WithoutAuth_SendsNoAuthorizationHeader()
    {
        using var handler = new StubHandler();
        using var client = new FlinkRestClient(handler, BaseUrl, authType: "none");

        await client.GetClusterOverviewAsync(CancellationToken.None);

        Assert.Null(Assert.Single(handler.Requests).Authorization);
    }

    [Fact]
    public async Task GetJobMetricsAsync_AppendsTheFilterOnlyWhenOneIsGiven()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.OK, "[]");
        handler.EnqueueResponse(HttpStatusCode.OK, "[]");
        using var client = new FlinkRestClient(handler, BaseUrl);

        await client.GetJobMetricsAsync("job-1", "numRecordsIn", CancellationToken.None);
        await client.GetJobMetricsAsync("job-1", null, CancellationToken.None);

        Assert.Equal(
            "http://localhost:8081/jobs/job-1/metrics?get=numRecordsIn",
            handler.Requests[0].Url?.AbsoluteUri);
        Assert.Equal("http://localhost:8081/jobs/job-1/metrics", handler.Requests[1].Url?.AbsoluteUri);
    }

    [Fact]
    public async Task CancelJobAsync_PatchesTheJobWithCancelMode()
    {
        using var handler = new StubHandler();
        using var client = new FlinkRestClient(handler, BaseUrl);

        await client.CancelJobAsync("job-1", CancellationToken.None);

        var recorded = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Patch, recorded.Method);
        Assert.Equal("http://localhost:8081/jobs/job-1?mode=cancel", recorded.Url?.AbsoluteUri);
    }

    [Fact]
    public async Task RescaleJobAsync_PatchesTheJobWithTheNewParallelism()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.OK, """{"request-id":"trigger-1"}""");
        using var client = new FlinkRestClient(handler, BaseUrl);

        var response = await client.RescaleJobAsync("job-1", 4, CancellationToken.None);

        var recorded = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Patch, recorded.Method);
        Assert.Equal("http://localhost:8081/jobs/job-1/rescaling?parallelism=4", recorded.Url?.AbsoluteUri);
        Assert.Equal("trigger-1", response.RequestId);
    }

    [Fact]
    public async Task RunJarAsync_PostsOnlyTheFieldsThatAreSet()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.OK, """{"jobId":"job-9"}""");
        using var client = new FlinkRestClient(handler, BaseUrl);

        var response = await client.RunJarAsync(
            "jar-1",
            new JarRunRequest { EntryClass = "com.example.Main", Parallelism = 2 },
            CancellationToken.None);

        var recorded = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, recorded.Method);
        Assert.Equal("http://localhost:8081/jars/jar-1/run", recorded.Url?.AbsoluteUri);
        Assert.Equal("job-9", response.JobId);

        using var body = JsonDocument.Parse(recorded.Body);
        Assert.Equal("com.example.Main", body.RootElement.GetProperty("entryClass").GetString());
        Assert.Equal(2, body.RootElement.GetProperty("parallelism").GetInt32());

        // Unset fields stay out of the request so Flink keeps its own defaults.
        Assert.False(body.RootElement.TryGetProperty("programArgs", out _));
        Assert.False(body.RootElement.TryGetProperty("savepointPath", out _));
    }

    [Fact]
    public async Task TriggerSavepointAsync_PostsTheTargetDirectoryAndCancelFlag()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.OK, """{"request-id":"trigger-2"}""");
        using var client = new FlinkRestClient(handler, BaseUrl);

        var response = await client.TriggerSavepointAsync("job-1", "/savepoints", cancelJob: true, CancellationToken.None);

        var recorded = Assert.Single(handler.Requests);
        Assert.Equal("http://localhost:8081/jobs/job-1/savepoints", recorded.Url?.AbsoluteUri);
        Assert.Equal("trigger-2", response.RequestId);

        using var body = JsonDocument.Parse(recorded.Body);
        Assert.Equal("/savepoints", body.RootElement.GetProperty("targetDirectory").GetString());
        Assert.True(body.RootElement.GetProperty("cancelJob").GetBoolean());
    }

    [Fact]
    public async Task AnErrorFromFlink_SurfacesAsAnHttpRequestException()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.InternalServerError, "job manager unreachable");
        using var client = new FlinkRestClient(handler, BaseUrl);

        await Assert.ThrowsAsync<HttpRequestException>(
            () => client.GetClusterOverviewAsync(CancellationToken.None));
    }

    private sealed record RecordedRequest(HttpMethod Method, Uri? Url, string? Authorization, string Body);

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

            Requests.Add(new RecordedRequest(
                request.Method,
                request.RequestUri,
                request.Headers.Authorization?.ToString(),
                body));

            var response = _responses.Count > 0
                ? _responses.Dequeue()
                : (Status: HttpStatusCode.OK, Body: "{}");

            return new HttpResponseMessage(response.Status) { Content = new StringContent(response.Body) };
        }
    }
}
