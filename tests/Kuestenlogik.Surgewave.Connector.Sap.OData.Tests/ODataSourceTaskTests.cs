using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.OData.Tests;

/// <summary>
/// Tests for <see cref="ODataSourceTask"/> driven through a stub transport: how the task
/// authenticates against SAP, which headers a Gateway service needs to see, and what happens
/// when the service says no. A poll that quietly returns nothing looks healthy forever.
/// </summary>
/// <remarks>
/// Shares a collection with the sink tests because Simple.OData.Client keeps a process-wide
/// metadata cache keyed by service URL.
/// </remarks>
[Collection("SapODataClient")]
public class ODataSourceTaskTests
{
    private const string ServiceUrl = "http://sap.invalid/sap/opu/odata/sap/ZORDERS_SRV/";
    private const string TokenUrl = "http://sap.invalid/oauth/token";

    [Fact]
    public void Start_WithOAuthButWithoutATokenEndpoint_FailsBeforeAnyRequest()
    {
        using var handler = new StubHandler(Route);
        using var task = new ODataSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = SourceConfig();
        config[ODataConnectorConfig.AuthType] = "oauth";
        config[ODataConnectorConfig.OAuthClientId] = "client-1";

        // "oauth" used to be an empty stub that handed back an unauthenticated client and
        // let every request fail later; a half-configured OAuth setup has to fail loudly.
        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));

        Assert.Contains(ODataConnectorConfig.OAuthTokenUrl, ex.Message, StringComparison.Ordinal);
        Assert.Empty(handler.Requests);
    }

    [Fact]
    public async Task PollAsync_WithOAuth_ExchangesTheClientCredentialsForABearerToken()
    {
        using var handler = new StubHandler(Route);
        var errors = new List<Exception>();
        using var task = new ODataSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(OAuthConfig());

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        var token = Assert.Single(handler.Requests, r => r.Uri == TokenUrl);
        Assert.Equal(HttpMethod.Post, token.Method);
        Assert.Contains("grant_type=client_credentials", token.Body, StringComparison.Ordinal);
        Assert.Contains("client_id=client-1", token.Body, StringComparison.Ordinal);
        Assert.Contains("client_secret=s3cret", token.Body, StringComparison.Ordinal);

        // The entity read against the stub fails, and that failure is reported rather than
        // hidden - but the token exchange happened first, which is the point here.
        Assert.Empty(records);
        Assert.NotEmpty(errors);
    }

    [Fact]
    public async Task PollAsync_ReusesTheAccessTokenUntilItExpires()
    {
        using var handler = new StubHandler(Route);
        using var task = new ODataSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = OAuthConfig();
        config[ODataConnectorConfig.PollIntervalMs] = "0";
        task.Start(config);

        await task.PollAsync(TestContext.Current.CancellationToken);
        await task.PollAsync(TestContext.Current.CancellationToken);

        // The token is good for an hour, so a second poll must not pay for another round trip.
        Assert.Single(handler.Requests, r => r.Uri == TokenUrl);
    }

    [Fact]
    public async Task Start_WithChangeTrackingEnabled_AsksTheServiceForADeltaLink()
    {
        using var handler = new StubHandler(Route);
        using var task = new ODataSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = OAuthConfig();
        config[ODataConnectorConfig.DeltaLink] = "true";
        task.Start(config);

        await task.PollAsync(TestContext.Current.CancellationToken);

        // odata.use.delta used to be parsed into a field nothing ever read. A service only
        // hands out a delta link when the client asks it to track changes.
        var token = Assert.Single(handler.Requests, r => r.Uri == TokenUrl);
        Assert.Equal("odata.track-changes", token.Headers["Prefer"]);
    }

    [Fact]
    public async Task Start_WithoutChangeTracking_DoesNotAskForADeltaLink()
    {
        using var handler = new StubHandler(Route);
        using var task = new ODataSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(OAuthConfig());

        await task.PollAsync(TestContext.Current.CancellationToken);

        var token = Assert.Single(handler.Requests, r => r.Uri == TokenUrl);
        Assert.DoesNotContain("Prefer", token.Headers.Keys);
    }

    [Theory]
    [InlineData(null, "100")]
    [InlineData("200", "200")]
    public async Task PollAsync_SendsTheSapClientEveryRequestNeeds(string? configured, string expected)
    {
        using var handler = new StubHandler(Route);
        using var task = new ODataSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = OAuthConfig();
        if (configured != null)
        {
            config["sap.client"] = configured;
        }

        task.Start(config);

        await task.PollAsync(TestContext.Current.CancellationToken);

        // An SAP Gateway request without sap-client lands in whatever client the service
        // user defaults to - which is rarely the one the pipeline was configured for.
        var token = Assert.Single(handler.Requests, r => r.Uri == TokenUrl);
        Assert.Equal(expected, token.Headers["sap-client"]);
        Assert.Equal("application/json", token.Headers["Accept"]);
    }

    [Fact]
    public async Task PollAsync_WhenTheServiceRejectsTheCredentials_SurfacesTheError()
    {
        using var handler = new StubHandler(_ => Json(HttpStatusCode.Unauthorized, """{"error":"bad credentials"}"""));
        var errors = new List<Exception>();
        using var task = new ODataSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.Start(SourceConfig());

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        // Poll used to swallow every exception with no logger at all, so a wrong password
        // showed up as a permanently healthy task that produced nothing.
        Assert.Empty(records);
        Assert.NotEmpty(errors);
    }

    [Fact]
    public async Task PollAsync_WithinThePollInterval_DoesNotTouchTheService()
    {
        using var handler = new StubHandler(Route);
        using var task = new ODataSourceTask(handler);
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        var config = OAuthConfig();
        config[ODataConnectorConfig.PollIntervalMs] = "60000";
        task.Start(config);

        await task.PollAsync(TestContext.Current.CancellationToken);
        var afterFirstPoll = handler.Requests.Count;

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Empty(records);
        Assert.Equal(afterFirstPoll, handler.Requests.Count);
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [ODataConnectorConfig.Topic] = "sap-orders",
        [ODataConnectorConfig.ServiceUrl] = ServiceUrl,
        [ODataConnectorConfig.EntitySet] = "SalesOrderSet",
        [ODataConnectorConfig.Username] = "SAPUSER",
        [ODataConnectorConfig.Password] = "s3cret",
        [ODataConnectorConfig.PollIntervalMs] = "0"
    };

    private static Dictionary<string, string> OAuthConfig()
    {
        var config = SourceConfig();
        config[ODataConnectorConfig.AuthType] = "oauth";
        config[ODataConnectorConfig.OAuthTokenUrl] = TokenUrl;
        config[ODataConnectorConfig.OAuthClientId] = "client-1";
        config[ODataConnectorConfig.OAuthClientSecret] = "s3cret";
        return config;
    }

    private static HttpResponseMessage Route(HttpRequestMessage request)
        => request.RequestUri?.OriginalString == TokenUrl
            ? Json(HttpStatusCode.OK, """{"access_token":"token-1","expires_in":3600}""")
            : Json(HttpStatusCode.InternalServerError, """{"error":"service unavailable"}""");

    private static HttpResponseMessage Json(HttpStatusCode status, string json)
        => new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };

    private sealed class CapturedRequest
    {
        public required HttpMethod Method { get; init; }

        public required string Uri { get; init; }

        public required string Body { get; init; }

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
                Uri = request.RequestUri?.OriginalString ?? string.Empty,
                Body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken),
                Headers = request.Headers.ToDictionary(h => h.Key, h => string.Join(",", h.Value), StringComparer.OrdinalIgnoreCase)
            });

            return responder(request);
        }
    }
}
