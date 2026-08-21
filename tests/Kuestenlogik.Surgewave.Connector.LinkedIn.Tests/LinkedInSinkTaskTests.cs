using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.LinkedIn.Tests;

/// <summary>
/// Wire format and failure handling of the LinkedIn ugcPosts sink. The union keys have to reach
/// LinkedIn verbatim, and a rejected post must not be silently committed.
/// </summary>
public class LinkedInSinkTaskTests
{
    private const string TextPayload = """{"text":"hello from surgewave"}""";

    [Fact]
    public async Task PutAsync_SendsUgcPostWithDottedUnionKeys()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.Created, """{"id":"urn:li:share:1"}""");

        using var task = new LinkedInSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record(TextPayload)], cts.Token);

        var request = Assert.Single(handler.Requests);
        Assert.Equal("https://api.linkedin.com/v2/ugcPosts", request.Url);
        Assert.Contains("\"author\":\"urn:li:organization:12345\"", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"lifecycleState\":\"PUBLISHED\"", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"com.linkedin.ugc.ShareContent\"", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"shareCommentary\":{\"text\":\"hello from surgewave\"}", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"shareMediaCategory\":\"NONE\"", request.Body, StringComparison.Ordinal);
        Assert.Contains("\"com.linkedin.ugc.MemberNetworkVisibility\":\"PUBLIC\"", request.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_AuthenticatesWithBearerTokenAndRestliVersion()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.Created, "{}");

        using var task = new LinkedInSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record(TextPayload)], cts.Token);

        var request = Assert.Single(handler.Requests);
        Assert.Equal("Bearer token-abc", request.Headers["Authorization"]);
        Assert.Equal("2.0.0", request.Headers["X-Restli-Protocol-Version"]);
    }

    [Fact]
    public async Task PutAsync_PostsAsPerson_WhenNoOrganizationIsConfigured()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.Created, "{}");

        var config = SinkConfig();
        config.Remove(LinkedInConnectorConfig.OrganizationId);
        config[LinkedInConnectorConfig.PersonId] = "abcd1234";

        using var task = new LinkedInSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record(TextPayload)], cts.Token);

        Assert.Contains("\"author\":\"urn:li:person:abcd1234\"", Assert.Single(handler.Requests).Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_SkipsRecords_WhenNoAuthorIsConfigured()
    {
        using var handler = new StubHandler();

        var config = SinkConfig();
        config.Remove(LinkedInConnectorConfig.OrganizationId);

        using var task = new LinkedInSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record(TextPayload)], cts.Token);

        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_ThrowsAndStopsTheBatch_WhenLinkedInRejectsThePost()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.Unauthorized, """{"message":"expired token"}""");

        using var task = new LinkedInSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([Record(TextPayload), Record(TextPayload, offset: 1)], cts.Token));

        Assert.Single(handler.Requests);
        Assert.Single(errors);
    }

    [Fact]
    public async Task PutAsync_ReadsTextFromConfiguredField()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.Created, "{}");

        var config = SinkConfig();
        config[LinkedInConnectorConfig.TextField] = "message";

        using var task = new LinkedInSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"message":"from custom field"}""")], cts.Token);

        Assert.Contains("\"text\":\"from custom field\"", Assert.Single(handler.Requests).Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_SkipsRecordsWithEmptyText()
    {
        using var handler = new StubHandler();

        using var task = new LinkedInSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(SinkConfig());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"text":""}""")], cts.Token);

        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_UsesConfiguredDefaultVisibility()
    {
        using var handler = new StubHandler();
        handler.Enqueue(HttpStatusCode.Created, "{}");

        var config = SinkConfig();
        config[LinkedInConnectorConfig.DefaultVisibility] = "CONNECTIONS";

        using var task = new LinkedInSinkTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record(TextPayload)], cts.Token);

        Assert.Contains(
            "\"com.linkedin.ugc.MemberNetworkVisibility\":\"CONNECTIONS\"",
            Assert.Single(handler.Requests).Body,
            StringComparison.Ordinal);
    }

    private static Dictionary<string, string> SinkConfig() => new()
    {
        [LinkedInConnectorConfig.AccessToken] = "token-abc",
        [LinkedInConnectorConfig.OrganizationId] = "12345"
    };

    private static SinkRecord Record(string json, long offset = 0) => new()
    {
        Topic = "linkedin-posts",
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

            var headers = request.Headers.ToDictionary(
                header => header.Key,
                header => string.Join(",", header.Value),
                StringComparer.OrdinalIgnoreCase);

            Requests.Add(new CapturedRequest(request.RequestUri?.ToString() ?? string.Empty, body, headers));

            return _responses.Count > 0
                ? _responses.Dequeue()
                : new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("{}", Encoding.UTF8, "application/json")
                };
        }
    }

    private sealed record CapturedRequest(string Url, string Body, IReadOnlyDictionary<string, string> Headers);
}
