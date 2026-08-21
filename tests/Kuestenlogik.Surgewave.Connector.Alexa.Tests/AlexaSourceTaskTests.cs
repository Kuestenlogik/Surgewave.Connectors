using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Alexa.Tests;

/// <summary>
/// Tests for <see cref="AlexaSourceTask"/> driven through a stubbed HTTP transport.
/// </summary>
public class AlexaSourceTaskTests
{
    private const string TokenJson = """{"access_token":"token-abc","expires_in":3600}""";

    private const string EndpointsJson = """
        {"endpoints":[{"endpointId":"light-1","friendlyName":"Kitchen","displayCategories":["LIGHT"]}]}
        """;

    private const string StateJson = """
        {"properties":[{"namespace":"Alexa.PowerController","name":"powerState","value":"ON"}]}
        """;

    private static Dictionary<string, string> Config(
        string pollIntervalMs = "0",
        string includeLights = "true",
        string filterEndpointIds = "") =>
        new()
        {
            [AlexaConnectorConfig.ClientId] = "client-id",
            [AlexaConnectorConfig.ClientSecret] = "client-secret",
            [AlexaConnectorConfig.RefreshToken] = "refresh-token",
            [AlexaConnectorConfig.Topic] = "alexa-events",
            [AlexaConnectorConfig.PollIntervalMs] = pollIntervalMs,
            [AlexaConnectorConfig.IncludeLights] = includeLights,
            [AlexaConnectorConfig.FilterEndpointIds] = filterEndpointIds
        };

    private static HttpResponseMessage JsonResponse(string json) =>
        new(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private static HttpResponseMessage Respond(HttpRequestMessage request)
    {
        var uri = request.RequestUri!.AbsoluteUri;
        if (uri.StartsWith("https://api.amazon.com/", StringComparison.Ordinal))
            return JsonResponse(TokenJson);
        if (uri.EndsWith("/v1/endpoints", StringComparison.Ordinal))
            return JsonResponse(EndpointsJson);
        return JsonResponse(StateJson);
    }

    [Fact]
    public async Task PollAsync_EmitsADeviceRecordForEveryIncludedEndpoint()
    {
        using var handler = new StubHandler(Respond);
        using var task = new AlexaSourceTask();
        task.StartWith(Config(), new HttpClient(handler));

        var records = await task.PollAsync(CancellationToken.None);

        var record = Assert.Single(records);
        Assert.Equal("alexa-events", record.Topic);
        Assert.Equal("alexa:light-1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("light-1", record.SourceOffset["endpoint_id"]);
        Assert.Equal("LIGHT", Encoding.UTF8.GetString(record.Headers!["alexa.category"]));
        Assert.Equal("Kitchen", Encoding.UTF8.GetString(record.Headers["alexa.name"]));

        using var doc = JsonDocument.Parse(Encoding.UTF8.GetString(record.Value));
        Assert.Equal("light-1", doc.RootElement.GetProperty("endpointId").GetString());
        Assert.Equal("Kitchen", doc.RootElement.GetProperty("friendlyName").GetString());
        Assert.Equal(
            "ON",
            doc.RootElement.GetProperty("state").GetProperty("Alexa.PowerController.powerState").GetString());
    }

    [Fact]
    public async Task PollAsync_ReportsAFailedTokenRefreshInsteadOfStayingSilent()
    {
        // A swallowed failure made bad credentials look like an idle account forever.
        var errors = new List<Exception>();
        using var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.Unauthorized));
        using var task = new AlexaSourceTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), new HttpClient(handler));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Empty(records);
        Assert.IsType<HttpRequestException>(Assert.Single(errors));
    }

    [Fact]
    public async Task PollAsync_ReportsARejectedEndpointsRequest()
    {
        var errors = new List<Exception>();
        using var handler = new StubHandler(request =>
            request.RequestUri!.AbsoluteUri.StartsWith("https://api.amazon.com/", StringComparison.Ordinal)
                ? JsonResponse(TokenJson)
                : new HttpResponseMessage(HttpStatusCode.Forbidden));
        using var task = new AlexaSourceTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), new HttpClient(handler));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Empty(records);
        var error = Assert.IsType<HttpRequestException>(Assert.Single(errors));
        Assert.Contains("403", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_SkipsCategoriesThatAreTurnedOff()
    {
        using var handler = new StubHandler(Respond);
        using var task = new AlexaSourceTask();
        task.StartWith(Config(includeLights: "false"), new HttpClient(handler));

        Assert.Empty(await task.PollAsync(CancellationToken.None));
    }

    [Fact]
    public async Task PollAsync_SkipsEndpointsOutsideTheConfiguredFilter()
    {
        using var handler = new StubHandler(Respond);
        using var task = new AlexaSourceTask();
        task.StartWith(Config(filterEndpointIds: "light-9"), new HttpClient(handler));

        Assert.Empty(await task.PollAsync(CancellationToken.None));
    }

    [Fact]
    public async Task PollAsync_EmitsNothingForAnUnchangedDeviceState()
    {
        using var handler = new StubHandler(Respond);
        using var task = new AlexaSourceTask();
        task.StartWith(Config(), new HttpClient(handler));

        Assert.Single(await task.PollAsync(CancellationToken.None));
        Assert.Empty(await task.PollAsync(CancellationToken.None));
    }

    [Fact]
    public async Task PollAsync_DoesNotCallTheApiBeforeThePollIntervalElapsed()
    {
        using var handler = new StubHandler(Respond);
        using var task = new AlexaSourceTask();
        task.StartWith(Config(pollIntervalMs: "60000"), new HttpClient(handler));

        Assert.Single(await task.PollAsync(CancellationToken.None));
        var callsAfterFirstPoll = handler.Requests.Count;

        Assert.Empty(await task.PollAsync(CancellationToken.None));
        Assert.Equal(callsAfterFirstPoll, handler.Requests.Count);
    }

    /// <summary>
    /// Canned HTTP transport: records every outgoing request URL and answers from a responder.
    /// </summary>
    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _responder;

        public StubHandler(Func<HttpRequestMessage, HttpResponseMessage> responder)
        {
            _responder = responder;
        }

        public List<string> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request.RequestUri!.AbsoluteUri);
            return Task.FromResult(_responder(request));
        }
    }
}
