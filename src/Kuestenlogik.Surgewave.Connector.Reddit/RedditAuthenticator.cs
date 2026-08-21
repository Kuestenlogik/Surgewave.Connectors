using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Connector.Reddit;

/// <summary>
/// Obtains Reddit OAuth access tokens via the password grant used by script apps.
/// Reddit.NET does not fetch tokens itself, so the token must be acquired up front.
/// </summary>
internal static class RedditAuthenticator
{
    private static readonly Uri TokenEndpoint = new("https://www.reddit.com/api/v1/access_token");

    public static async Task<(string AccessToken, DateTimeOffset ExpiresAt)> FetchAccessTokenAsync(
        string clientId,
        string? clientSecret,
        string username,
        string password,
        string userAgent,
        CancellationToken cancellationToken)
    {
        using var http = new HttpClient();
        return await FetchAccessTokenAsync(http, clientId, clientSecret, username, password, userAgent, cancellationToken);
    }

    /// <summary>
    /// Fetches an access token over an already-built <see cref="HttpClient"/>.
    /// </summary>
    public static async Task<(string AccessToken, DateTimeOffset ExpiresAt)> FetchAccessTokenAsync(
        HttpClient http,
        string clientId,
        string? clientSecret,
        string username,
        string password,
        string userAgent,
        CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, TokenEndpoint);
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}")));
        request.Headers.TryAddWithoutValidation("User-Agent", userAgent);
        request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "password",
            ["username"] = username,
            ["password"] = password
        });

        using var response = await http.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        using var doc = JsonDocument.Parse(body);

        // Reddit reports invalid credentials as 200 OK with an "error" field
        if (doc.RootElement.TryGetProperty("error", out var error))
        {
            throw new InvalidOperationException($"Reddit authentication failed: {error}");
        }

        if (!doc.RootElement.TryGetProperty("access_token", out var token) ||
            token.GetString() is not { Length: > 0 } accessToken)
        {
            throw new InvalidOperationException("Reddit token response contained no access_token");
        }

        var expiresInSeconds = doc.RootElement.TryGetProperty("expires_in", out var expiresIn) &&
                               expiresIn.ValueKind == JsonValueKind.Number &&
                               expiresIn.TryGetInt32(out var seconds)
            ? seconds
            : 3600;

        return (accessToken, DateTimeOffset.UtcNow.AddSeconds(expiresInSeconds));
    }
}
