using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Alexa.Tests;

/// <summary>
/// Tests for <see cref="AlexaSinkTask"/> driven through a stubbed HTTP transport.
/// </summary>
public class AlexaSinkTaskTests
{
    private const string TokenJson = """{"access_token":"token-abc","expires_in":3600}""";

    private static Dictionary<string, string> Config(string region = "NA", string defaultEndpointId = "") =>
        new()
        {
            [AlexaConnectorConfig.ClientId] = "client-id",
            [AlexaConnectorConfig.ClientSecret] = "client-secret",
            [AlexaConnectorConfig.RefreshToken] = "refresh-token",
            [AlexaConnectorConfig.Region] = region,
            [AlexaConnectorConfig.DefaultEndpointId] = defaultEndpointId
        };

    private static SinkRecord CreateRecord(string json, long offset = 0, IReadOnlyDictionary<string, byte[]>? headers = null) =>
        new()
        {
            Topic = "commands",
            Partition = 0,
            Offset = offset,
            Value = Encoding.UTF8.GetBytes(json),
            Headers = headers
        };

    private static bool IsTokenRequest(HttpRequestMessage request) =>
        request.RequestUri!.AbsoluteUri.StartsWith("https://api.amazon.com/", StringComparison.Ordinal);

    private static HttpResponseMessage JsonResponse(HttpStatusCode status, string json) =>
        new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    [Fact]
    public async Task PutAsync_FailsTheBatchWhenTheTokenRefreshFails()
    {
        // A swallowed refresh failure would ack commands that were never sent.
        using var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.Unauthorized));
        var errors = new List<Exception>();
        using var task = new AlexaSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), new HttpClient(handler));

        await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([CreateRecord("""{"endpointId":"light-1","on":true}""")], CancellationToken.None));

        Assert.Single(errors);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_FailsTheBatchWhenTheDirectiveIsRejected()
    {
        using var handler = new StubHandler(request => IsTokenRequest(request)
            ? JsonResponse(HttpStatusCode.OK, TokenJson)
            : new HttpResponseMessage(HttpStatusCode.InternalServerError));
        var errors = new List<Exception>();
        using var task = new AlexaSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), new HttpClient(handler));

        var ex = await Assert.ThrowsAsync<HttpRequestException>(
            () => task.PutAsync([CreateRecord("""{"endpointId":"light-1","on":true}""")], CancellationToken.None));

        Assert.Contains("light-1", ex.Message, StringComparison.Ordinal);
        Assert.Contains("500", ex.Message, StringComparison.Ordinal);
        Assert.Single(errors);
    }

    [Fact]
    public async Task PutAsync_PostsAPowerDirectiveToTheAddressedEndpoint()
    {
        using var handler = new StubHandler(request => IsTokenRequest(request)
            ? JsonResponse(HttpStatusCode.OK, TokenJson)
            : new HttpResponseMessage(HttpStatusCode.OK));
        using var task = new AlexaSinkTask();
        task.StartWith(Config(), new HttpClient(handler));

        await task.PutAsync([CreateRecord("""{"endpointId":"light-1","on":true}""")], CancellationToken.None);

        Assert.Equal(2, handler.Requests.Count);
        var directive = handler.Requests[1];
        Assert.Equal("POST", directive.Method);
        Assert.Equal("https://api.amazonalexa.com/v3/appliances/light-1/directives", directive.Uri);
        Assert.Equal("token-abc", directive.Token);

        using var doc = JsonDocument.Parse(directive.Body);
        var root = doc.RootElement.GetProperty("directive");
        Assert.Equal("Alexa.PowerController", root.GetProperty("header").GetProperty("namespace").GetString());
        Assert.Equal("TurnOn", root.GetProperty("header").GetProperty("name").GetString());
        Assert.Equal("light-1", root.GetProperty("endpoint").GetProperty("endpointId").GetString());
    }

    [Fact]
    public async Task PutAsync_UsesTheRegionalApiEndpoint()
    {
        using var handler = new StubHandler(request => IsTokenRequest(request)
            ? JsonResponse(HttpStatusCode.OK, TokenJson)
            : new HttpResponseMessage(HttpStatusCode.OK));
        using var task = new AlexaSinkTask();
        task.StartWith(Config(region: "EU"), new HttpClient(handler));

        await task.PutAsync([CreateRecord("""{"endpointId":"lock-1","lock":true}""")], CancellationToken.None);

        Assert.Equal("https://api.eu.amazonalexa.com/v3/appliances/lock-1/directives", handler.Requests[1].Uri);
    }

    [Fact]
    public async Task PutAsync_InfersTheBrightnessDirectiveAndItsPayload()
    {
        using var handler = new StubHandler(request => IsTokenRequest(request)
            ? JsonResponse(HttpStatusCode.OK, TokenJson)
            : new HttpResponseMessage(HttpStatusCode.OK));
        using var task = new AlexaSinkTask();
        task.StartWith(Config(), new HttpClient(handler));

        await task.PutAsync([CreateRecord("""{"endpointId":"light-1","brightness":42}""")], CancellationToken.None);

        using var doc = JsonDocument.Parse(handler.Requests[1].Body);
        var root = doc.RootElement.GetProperty("directive");
        Assert.Equal("Alexa.BrightnessController", root.GetProperty("header").GetProperty("namespace").GetString());
        Assert.Equal("SetBrightness", root.GetProperty("header").GetProperty("name").GetString());
        Assert.Equal(42, root.GetProperty("payload").GetProperty("brightness").GetInt32());
    }

    [Fact]
    public async Task PutAsync_TakesTheEndpointFromTheRecordHeaderWhenThePayloadHasNone()
    {
        using var handler = new StubHandler(request => IsTokenRequest(request)
            ? JsonResponse(HttpStatusCode.OK, TokenJson)
            : new HttpResponseMessage(HttpStatusCode.OK));
        using var task = new AlexaSinkTask();
        task.StartWith(Config(), new HttpClient(handler));

        var headers = new Dictionary<string, byte[]> { ["alexa.endpointId"] = Encoding.UTF8.GetBytes("plug-9") };

        await task.PutAsync([CreateRecord("""{"on":false}""", headers: headers)], CancellationToken.None);

        Assert.Equal("https://api.amazonalexa.com/v3/appliances/plug-9/directives", handler.Requests[1].Uri);
    }

    [Fact]
    public async Task PutAsync_ReportsAnUnaddressableRecordWithoutFailingTheBatch()
    {
        using var handler = new StubHandler(request => IsTokenRequest(request)
            ? JsonResponse(HttpStatusCode.OK, TokenJson)
            : new HttpResponseMessage(HttpStatusCode.OK));
        var errors = new List<Exception>();
        using var task = new AlexaSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), new HttpClient(handler));

        await task.PutAsync([CreateRecord("""{"on":true}""")], CancellationToken.None);

        Assert.IsType<InvalidOperationException>(Assert.Single(errors));
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task PutAsync_ReportsAPoisonRecordAndKeepsProcessingTheBatch()
    {
        using var handler = new StubHandler(request => IsTokenRequest(request)
            ? JsonResponse(HttpStatusCode.OK, TokenJson)
            : new HttpResponseMessage(HttpStatusCode.OK));
        var errors = new List<Exception>();
        using var task = new AlexaSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), new HttpClient(handler));

        await task.PutAsync(
            [CreateRecord("not json"), CreateRecord("""{"endpointId":"light-1","on":true}""", 1)],
            CancellationToken.None);

        // JsonDocument.Parse reports a reader-level subtype of JsonException.
        Assert.IsAssignableFrom<JsonException>(Assert.Single(errors));
        Assert.Equal(2, handler.Requests.Count);
        Assert.EndsWith("/light-1/directives", handler.Requests[1].Uri, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_ReusesTheAccessTokenAcrossBatches()
    {
        using var handler = new StubHandler(request => IsTokenRequest(request)
            ? JsonResponse(HttpStatusCode.OK, TokenJson)
            : new HttpResponseMessage(HttpStatusCode.OK));
        using var task = new AlexaSinkTask();
        task.StartWith(Config(), new HttpClient(handler));

        var record = CreateRecord("""{"endpointId":"light-1","on":true}""");
        await task.PutAsync([record], CancellationToken.None);
        await task.PutAsync([record], CancellationToken.None);

        Assert.Single(handler.Requests, r => IsTokenUri(r.Uri));
        Assert.Equal(3, handler.Requests.Count);
    }

    private static bool IsTokenUri(string uri) =>
        uri.StartsWith("https://api.amazon.com/", StringComparison.Ordinal);

    /// <summary>
    /// Canned HTTP transport: records every outgoing request and answers from a responder.
    /// </summary>
    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _responder;

        public StubHandler(Func<HttpRequestMessage, HttpResponseMessage> responder)
        {
            _responder = responder;
        }

        public List<(string Method, string Uri, string Body, string? Token)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content == null
                ? string.Empty
                : await request.Content.ReadAsStringAsync(cancellationToken);

            Requests.Add((
                request.Method.Method,
                request.RequestUri!.AbsoluteUri,
                body,
                request.Headers.Authorization?.Parameter));

            return _responder(request);
        }
    }
}
