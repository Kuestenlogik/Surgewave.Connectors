using System.Net;
using System.Text;

namespace Kuestenlogik.Surgewave.Connector.Reddit.Tests;

/// <summary>
/// Reddit.NET never fetches OAuth tokens on its own, so the connector runs the password grant
/// itself before it builds a client. These tests pin the shape of that exchange and the failure
/// modes that must not be mistaken for a successful login.
/// </summary>
public class RedditAuthenticatorTests
{
    [Fact]
    public async Task FetchAccessTokenAsync_PostsPasswordGrantWithBasicAuthAndUserAgent()
    {
        using var handler = new StubHandler(_ => JsonResponse(HttpStatusCode.OK, """{"access_token":"token-1","expires_in":3600}"""));
        using var http = new HttpClient(handler);

        var (accessToken, _) = await RedditAuthenticator.FetchAccessTokenAsync(
            http, "client-id", "client-secret", "spez", "hunter2", "Surgewave/1.0 by spez", CancellationToken.None);

        Assert.Equal("token-1", accessToken);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("https://www.reddit.com/api/v1/access_token", request.Uri);

        var expectedCredentials = Convert.ToBase64String(Encoding.UTF8.GetBytes("client-id:client-secret"));
        Assert.Equal("Basic " + expectedCredentials, request.Headers["Authorization"]);

        // Reddit's API rules require a distinct User-Agent - the configured one has to travel with
        // the request instead of being read from config and dropped.
        Assert.Equal("Surgewave/1.0 by spez", request.Headers["User-Agent"]);

        Assert.Contains("grant_type=password", request.Body, StringComparison.Ordinal);
        Assert.Contains("username=spez", request.Body, StringComparison.Ordinal);
        Assert.Contains("password=hunter2", request.Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FetchAccessTokenAsync_TakesExpiryFromExpiresIn()
    {
        using var handler = new StubHandler(_ => JsonResponse(HttpStatusCode.OK, """{"access_token":"token-1","expires_in":120}"""));
        using var http = new HttpClient(handler);

        var before = DateTimeOffset.UtcNow;
        var (_, expiresAt) = await RedditAuthenticator.FetchAccessTokenAsync(
            http, "client-id", "client-secret", "spez", "hunter2", "Surgewave/1.0", CancellationToken.None);
        var after = DateTimeOffset.UtcNow;

        Assert.InRange(expiresAt, before.AddSeconds(120), after.AddSeconds(120));
    }

    [Fact]
    public async Task FetchAccessTokenAsync_FallsBackToOneHour_WhenExpiresInIsMissing()
    {
        using var handler = new StubHandler(_ => JsonResponse(HttpStatusCode.OK, """{"access_token":"token-1"}"""));
        using var http = new HttpClient(handler);

        var before = DateTimeOffset.UtcNow;
        var (_, expiresAt) = await RedditAuthenticator.FetchAccessTokenAsync(
            http, "client-id", "client-secret", "spez", "hunter2", "Surgewave/1.0", CancellationToken.None);
        var after = DateTimeOffset.UtcNow;

        Assert.InRange(expiresAt, before.AddSeconds(3600), after.AddSeconds(3600));
    }

    [Fact]
    public async Task FetchAccessTokenAsync_RejectsErrorPayloadServedWithStatus200()
    {
        // Reddit answers bad credentials with 200 OK and an "error" field; treating that as a
        // token would leave every later API call unauthorized.
        using var handler = new StubHandler(_ => JsonResponse(HttpStatusCode.OK, """{"error":"invalid_grant"}"""));
        using var http = new HttpClient(handler);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => RedditAuthenticator.FetchAccessTokenAsync(
                http, "client-id", "client-secret", "spez", "hunter2", "Surgewave/1.0", CancellationToken.None));

        Assert.Contains("invalid_grant", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FetchAccessTokenAsync_RejectsResponseWithoutAccessToken()
    {
        using var handler = new StubHandler(_ => JsonResponse(HttpStatusCode.OK, """{"token_type":"bearer"}"""));
        using var http = new HttpClient(handler);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => RedditAuthenticator.FetchAccessTokenAsync(
                http, "client-id", "client-secret", "spez", "hunter2", "Surgewave/1.0", CancellationToken.None));
    }

    [Fact]
    public async Task FetchAccessTokenAsync_SurfacesHttpFailures()
    {
        using var handler = new StubHandler(_ => JsonResponse(HttpStatusCode.Unauthorized, """{"message":"Unauthorized"}"""));
        using var http = new HttpClient(handler);

        await Assert.ThrowsAsync<HttpRequestException>(
            () => RedditAuthenticator.FetchAccessTokenAsync(
                http, "client-id", "client-secret", "spez", "hunter2", "Surgewave/1.0", CancellationToken.None));
    }

    private static HttpResponseMessage JsonResponse(HttpStatusCode status, string json)
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
                Uri = request.RequestUri?.ToString() ?? string.Empty,
                Body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(cancellationToken),
                // Join the way HttpClient serializes a header, so a User-Agent whose
                // product tokens HttpHeaders splits apart is compared as it is sent.
                Headers = request.Headers.ToDictionary(
                    h => h.Key,
                    h => string.Join(h.Key.Equals("User-Agent", StringComparison.OrdinalIgnoreCase) ? " " : ",", h.Value),
                    StringComparer.OrdinalIgnoreCase)
            });

            return responder(request);
        }
    }
}
