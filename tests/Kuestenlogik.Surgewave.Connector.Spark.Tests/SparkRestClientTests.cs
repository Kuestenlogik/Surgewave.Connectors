using System.Net;
using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Connector.Spark.Tests;

/// <summary>
/// Tests for <see cref="SparkRestClient"/> driven through a stub transport: the exact endpoint
/// each operation hits, what the request body carries, and which identifiers come back. A job
/// submitted against the wrong URL - or whose batch id is lost - can never be killed again.
/// </summary>
public class SparkRestClientTests
{
    private const string SparkUrl = "http://spark.invalid:8080";
    private const string LivyUrl = "http://livy.invalid:8998";

    [Fact]
    public async Task SubmitBatchAsync_PostsTheBatchToLivyAndReturnsTheIdItWasGiven()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.Created, """{"id":42,"state":"starting","appId":"application_1700_0001"}""");
        using var client = Client(handler);

        var batch = await client.SubmitBatchAsync(
            new LivyBatchRequest
            {
                File = "s3://jobs/etl.py",
                ClassName = "com.example.Etl",
                DriverMemory = "4g",
                NumExecutors = 12
            },
            TestContext.Current.CancellationToken);

        var recorded = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, recorded.Method);
        Assert.Equal("http://livy.invalid:8998/batches", recorded.Uri);

        using var body = JsonDocument.Parse(recorded.Body);
        Assert.Equal("s3://jobs/etl.py", body.RootElement.GetProperty("file").GetString());
        Assert.Equal("com.example.Etl", body.RootElement.GetProperty("className").GetString());
        Assert.Equal("4g", body.RootElement.GetProperty("driverMemory").GetString());
        Assert.Equal(12, body.RootElement.GetProperty("numExecutors").GetInt32());

        // The batch id is the only handle a later kill or status poll has on this job.
        Assert.Equal(42, batch.Id);
        Assert.Equal("starting", batch.State);
        Assert.Equal("application_1700_0001", batch.AppId);
    }

    [Fact]
    public async Task SubmitBatchAsync_LeavesOutTheFieldsThatWereNeverSet()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.Created, """{"id":1}""");
        using var client = Client(handler);

        await client.SubmitBatchAsync(
            new LivyBatchRequest { File = "s3://jobs/etl.py" },
            TestContext.Current.CancellationToken);

        // Sending nulls would override the cluster's own defaults for queue, proxy user
        // and executor sizing with nothing at all.
        using var body = JsonDocument.Parse(Assert.Single(handler.Requests).Body);
        Assert.False(body.RootElement.TryGetProperty("queue", out _));
        Assert.False(body.RootElement.TryGetProperty("proxyUser", out _));
        Assert.False(body.RootElement.TryGetProperty("numExecutors", out _));
    }

    [Fact]
    public async Task SubmitApplicationAsync_PostsToTheSparkSubmissionApiAndReturnsTheSubmissionId()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.OK, """{"action":"CreateSubmissionResponse","submissionId":"driver-20260821","success":true}""");
        using var client = Client(handler);

        var response = await client.SubmitApplicationAsync(
            new SparkSubmissionRequest { AppResource = "s3://jobs/etl.jar", MainClass = "com.example.Etl" },
            TestContext.Current.CancellationToken);

        var recorded = Assert.Single(handler.Requests);
        Assert.Equal("http://spark.invalid:8080/v1/submissions/create", recorded.Uri);

        using var body = JsonDocument.Parse(recorded.Body);
        Assert.Equal("CreateSubmissionRequest", body.RootElement.GetProperty("action").GetString());
        Assert.Equal("s3://jobs/etl.jar", body.RootElement.GetProperty("appResource").GetString());

        Assert.Equal("driver-20260821", response.SubmissionId);
        Assert.True(response.Success);
    }

    [Fact]
    public async Task KillApplicationAsync_PostsTheSubmissionIdToTheKillEndpoint()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.OK, """{"submissionId":"driver-1","success":true}""");
        using var client = Client(handler);

        await client.KillApplicationAsync("driver-1", TestContext.Current.CancellationToken);

        var recorded = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, recorded.Method);
        Assert.Equal("http://spark.invalid:8080/v1/submissions/kill/driver-1", recorded.Uri);
        Assert.Equal(string.Empty, recorded.Body);
    }

    [Fact]
    public async Task GetBatchLogAsync_AppendsOnlyTheRangeItWasGiven()
    {
        using var handler = new StubHandler();
        using var client = Client(handler);

        await client.GetBatchLogAsync(7, from: 10, size: 100, ct: TestContext.Current.CancellationToken);
        await client.GetBatchLogAsync(7, ct: TestContext.Current.CancellationToken);

        Assert.Equal("http://livy.invalid:8998/batches/7/log?from=10&size=100", handler.Requests[0].Uri);
        Assert.Equal("http://livy.invalid:8998/batches/7/log", handler.Requests[1].Uri);
    }

    [Fact]
    public async Task AUrlWithATrailingSlash_DoesNotProduceADoubleSlash()
    {
        using var handler = new StubHandler();
        using var client = Client(handler, sparkUrl: "http://spark.invalid:8080/", livyUrl: "http://livy.invalid:8998/");

        await client.GetClusterStatusAsync(TestContext.Current.CancellationToken);
        await client.GetSessionsAsync(TestContext.Current.CancellationToken);

        Assert.Equal("http://spark.invalid:8080/json", handler.Requests[0].Uri);
        Assert.Equal("http://livy.invalid:8998/sessions", handler.Requests[1].Uri);
    }

    [Fact]
    public async Task WithoutALivyUrl_TheLivyOperationsRefuseToRun()
    {
        using var handler = new StubHandler();
        using var client = Client(handler, livyUrl: null);

        // Falling through to the Spark master URL would submit a Livy batch request to an
        // endpoint that does not understand it.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => client.GetSessionsAsync(TestContext.Current.CancellationToken));
        Assert.Contains("Livy URL", ex.Message, StringComparison.Ordinal);

        // The Spark master API stays usable in that configuration.
        await client.GetClusterStatusAsync(TestContext.Current.CancellationToken);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task BasicAuth_SendsTheCredentialsOnEveryRequest()
    {
        using var handler = new StubHandler();
        using var client = Client(handler, authType: "Basic", username: "spark", password: "s3cret");

        await client.GetClusterStatusAsync(TestContext.Current.CancellationToken);

        var expected = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("spark:s3cret"));
        Assert.Equal(expected, Assert.Single(handler.Requests).Authorization);
    }

    [Fact]
    public async Task WithoutCredentials_SendsNoAuthorizationHeader()
    {
        using var handler = new StubHandler();
        using var client = Client(handler, authType: "none");

        await client.GetClusterStatusAsync(TestContext.Current.CancellationToken);

        Assert.Null(Assert.Single(handler.Requests).Authorization);
    }

    [Fact]
    public async Task AnErrorFromTheCluster_SurfacesAsAnHttpRequestException()
    {
        using var handler = new StubHandler();
        handler.EnqueueResponse(HttpStatusCode.ServiceUnavailable, "livy is restarting");
        using var client = Client(handler);

        // Deserializing an error page into a batch would hand the caller a job id of 0.
        await Assert.ThrowsAsync<HttpRequestException>(
            () => client.SubmitBatchAsync(new LivyBatchRequest { File = "s3://jobs/etl.py" }, TestContext.Current.CancellationToken));
    }

    private static SparkRestClient Client(
        StubHandler handler,
        string? sparkUrl = SparkUrl,
        string? livyUrl = LivyUrl,
        string? authType = null,
        string? username = null,
        string? password = null)
        => new(sparkUrl, livyUrl, 60000, authType, username, password, handler);

    private sealed class CapturedRequest
    {
        public required HttpMethod Method { get; init; }

        public required string Uri { get; init; }

        public required string Body { get; init; }

        public required string? Authorization { get; init; }
    }

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Queue<(HttpStatusCode Status, string Body)> _responses = new();

        public List<CapturedRequest> Requests { get; } = [];

        public void EnqueueResponse(HttpStatusCode status, string body) => _responses.Enqueue((status, body));

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(new CapturedRequest
            {
                Method = request.Method,
                Uri = request.RequestUri?.OriginalString ?? string.Empty,
                Body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken),
                Authorization = request.Headers.Authorization?.ToString()
            });

            var response = _responses.Count > 0 ? _responses.Dequeue() : (Status: HttpStatusCode.OK, Body: "{}");
            return new HttpResponseMessage(response.Status)
            {
                Content = new StringContent(response.Body, Encoding.UTF8, "application/json")
            };
        }
    }
}
