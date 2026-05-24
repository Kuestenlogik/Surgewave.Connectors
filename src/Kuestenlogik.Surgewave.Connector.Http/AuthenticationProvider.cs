namespace Kuestenlogik.Surgewave.Connector.Http;

using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;

/// <summary>
/// Interface for HTTP authentication providers.
/// </summary>
public interface IAuthenticationProvider
{
    /// <summary>
    /// Apply authentication to an HTTP request.
    /// </summary>
    /// <param name="request">The HTTP request message.</param>
    /// <param name="body">The request body (used for HMAC signing).</param>
    void ApplyAuthentication(HttpRequestMessage request, byte[]? body);
}

/// <summary>
/// No authentication.
/// </summary>
public sealed class NoAuthProvider : IAuthenticationProvider
{
    public static readonly NoAuthProvider Instance = new();

    private NoAuthProvider() { }

    public void ApplyAuthentication(HttpRequestMessage request, byte[]? body)
    {
        // No-op
    }
}

/// <summary>
/// HTTP Basic authentication (username:password base64 encoded).
/// </summary>
public sealed class BasicAuthProvider : IAuthenticationProvider
{
    private readonly string _credentials;

    public BasicAuthProvider(string username, string password)
    {
        var credentials = $"{username}:{password}";
        _credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes(credentials));
    }

    public void ApplyAuthentication(HttpRequestMessage request, byte[]? body)
    {
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", _credentials);
    }
}

/// <summary>
/// Bearer token authentication.
/// </summary>
public sealed class BearerAuthProvider : IAuthenticationProvider
{
    private readonly string _token;

    public BearerAuthProvider(string token)
    {
        _token = token;
    }

    public void ApplyAuthentication(HttpRequestMessage request, byte[]? body)
    {
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _token);
    }
}

/// <summary>
/// API Key authentication via custom header.
/// </summary>
public sealed class ApiKeyAuthProvider : IAuthenticationProvider
{
    private readonly string _headerName;
    private readonly string _apiKey;

    public ApiKeyAuthProvider(string apiKey, string headerName = "X-API-Key")
    {
        _apiKey = apiKey;
        _headerName = headerName;
    }

    public void ApplyAuthentication(HttpRequestMessage request, byte[]? body)
    {
        request.Headers.TryAddWithoutValidation(_headerName, _apiKey);
    }
}

/// <summary>
/// HMAC signature authentication for request signing.
/// </summary>
public sealed class HmacAuthProvider : IAuthenticationProvider
{
    private readonly byte[] _secret;
    private readonly string _headerName;
    private readonly string _algorithm;

    public HmacAuthProvider(string secret, string headerName = "X-Signature-256", string algorithm = "HMAC-SHA256")
    {
        _secret = Encoding.UTF8.GetBytes(secret);
        _headerName = headerName;
        _algorithm = algorithm;
    }

    public void ApplyAuthentication(HttpRequestMessage request, byte[]? body)
    {
        if (body == null || body.Length == 0)
            return;

        var signature = ComputeSignature(body);
        var prefix = _algorithm.ToLowerInvariant() switch
        {
            "hmac-sha256" => "sha256=",
            "hmac-sha1" => "sha1=",
            "hmac-sha512" => "sha512=",
            _ => ""
        };

        request.Headers.TryAddWithoutValidation(_headerName, $"{prefix}{signature}");
    }

    private string ComputeSignature(byte[] body)
    {
#pragma warning disable CA5350 // HMAC-SHA1 supported for legacy webhook compatibility (e.g., GitHub)
        using var hmac = _algorithm.ToUpperInvariant() switch
        {
            "HMAC-SHA256" => (HMAC)new HMACSHA256(_secret),
            "HMAC-SHA1" => new HMACSHA1(_secret),
            "HMAC-SHA512" => new HMACSHA512(_secret),
            _ => new HMACSHA256(_secret)
        };
#pragma warning restore CA5350

        var hash = hmac.ComputeHash(body);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}

/// <summary>
/// Factory for creating authentication providers from configuration.
/// </summary>
public static class AuthenticationProviderFactory
{
    public static IAuthenticationProvider Create(IDictionary<string, string> config)
    {
        var authType = config.TryGetValue(HttpConnectorConfig.AuthType, out var type)
            ? type
            : HttpConnectorConfig.AuthTypeNone;

        return authType switch
        {
            HttpConnectorConfig.AuthTypeBasic => CreateBasicAuth(config),
            HttpConnectorConfig.AuthTypeBearer => CreateBearerAuth(config),
            HttpConnectorConfig.AuthTypeApiKey => CreateApiKeyAuth(config),
            HttpConnectorConfig.AuthTypeHmac => CreateHmacAuth(config),
            _ => NoAuthProvider.Instance
        };
    }

    private static BasicAuthProvider CreateBasicAuth(IDictionary<string, string> config)
    {
        var username = config.TryGetValue(HttpConnectorConfig.AuthUsername, out var u) ? u : "";
        var password = config.TryGetValue(HttpConnectorConfig.AuthPassword, out var p) ? p : "";
        return new BasicAuthProvider(username, password);
    }

    private static BearerAuthProvider CreateBearerAuth(IDictionary<string, string> config)
    {
        var token = config.TryGetValue(HttpConnectorConfig.AuthToken, out var t) ? t : "";
        return new BearerAuthProvider(token);
    }

    private static ApiKeyAuthProvider CreateApiKeyAuth(IDictionary<string, string> config)
    {
        var apiKey = config.TryGetValue(HttpConnectorConfig.AuthApiKey, out var k) ? k : "";
        var header = config.TryGetValue(HttpConnectorConfig.AuthApiKeyHeader, out var h)
            ? h
            : HttpConnectorConfig.DefaultApiKeyHeader;
        return new ApiKeyAuthProvider(apiKey, header);
    }

    private static HmacAuthProvider CreateHmacAuth(IDictionary<string, string> config)
    {
        var secret = config.TryGetValue(HttpConnectorConfig.AuthHmacSecret, out var s) ? s : "";
        var header = config.TryGetValue(HttpConnectorConfig.AuthHmacHeader, out var h)
            ? h
            : HttpConnectorConfig.DefaultSignatureHeader;
        var algorithm = config.TryGetValue(HttpConnectorConfig.AuthHmacAlgorithm, out var a)
            ? a
            : HttpConnectorConfig.DefaultSignatureAlgorithm;
        return new HmacAuthProvider(secret, header, algorithm);
    }
}

/// <summary>
/// Utility for validating incoming webhook signatures.
/// </summary>
public static class SignatureValidator
{
    /// <summary>
    /// Validate an HMAC signature on incoming webhook request.
    /// </summary>
    public static bool ValidateSignature(byte[] body, string signature, string secret, string algorithm = "HMAC-SHA256")
    {
        if (string.IsNullOrEmpty(signature) || string.IsNullOrEmpty(secret))
            return false;

        var secretBytes = Encoding.UTF8.GetBytes(secret);

#pragma warning disable CA5350 // HMAC-SHA1 supported for legacy webhook compatibility (e.g., GitHub)
        using var hmac = algorithm.ToUpperInvariant() switch
        {
            "HMAC-SHA256" => (HMAC)new HMACSHA256(secretBytes),
            "HMAC-SHA1" => new HMACSHA1(secretBytes),
            "HMAC-SHA512" => new HMACSHA512(secretBytes),
            _ => new HMACSHA256(secretBytes)
        };
#pragma warning restore CA5350

        var computedHash = hmac.ComputeHash(body);
        var computed = Convert.ToHexString(computedHash).ToLowerInvariant();

        // Remove common prefixes like "sha256=" or "sha1="
        var expected = signature
            .Replace("sha256=", "", StringComparison.OrdinalIgnoreCase)
            .Replace("sha1=", "", StringComparison.OrdinalIgnoreCase)
            .Replace("sha512=", "", StringComparison.OrdinalIgnoreCase)
            .ToLowerInvariant();

        // Use constant-time comparison to prevent timing attacks
        return CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(computed),
            Encoding.UTF8.GetBytes(expected));
    }

    /// <summary>
    /// Validate timestamp to prevent replay attacks.
    /// </summary>
    public static bool ValidateTimestamp(string? timestampHeader, long toleranceMs)
    {
        if (string.IsNullOrEmpty(timestampHeader))
            return false;

        if (!long.TryParse(timestampHeader, out var timestamp))
            return false;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var age = now - timestamp;

        // Timestamp must be in the past but within tolerance
        return age >= 0 && age <= toleranceMs;
    }
}
