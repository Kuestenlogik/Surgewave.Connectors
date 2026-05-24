using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Kuestenlogik.Surgewave.Connector.Http;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Http.Tests;

/// <summary>
/// Tests for HTTP authentication providers.
/// </summary>
public sealed class AuthenticationProviderTests
{
    [Fact]
    public void NoAuthProvider_DoesNotModifyRequest()
    {
        // Arrange
        var provider = NoAuthProvider.Instance;
        using var request = new HttpRequestMessage(HttpMethod.Get, "https://example.com");
        var body = "test body"u8.ToArray();

        // Act
        provider.ApplyAuthentication(request, body);

        // Assert
        Assert.Null(request.Headers.Authorization);
    }

    [Fact]
    public void BasicAuthProvider_AddsAuthorizationHeader()
    {
        // Arrange
        var provider = new BasicAuthProvider("user", "password");
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.com");

        // Act
        provider.ApplyAuthentication(request, null);

        // Assert
        Assert.NotNull(request.Headers.Authorization);
        Assert.Equal("Basic", request.Headers.Authorization.Scheme);

        var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(request.Headers.Authorization.Parameter!));
        Assert.Equal("user:password", decoded);
    }

    [Fact]
    public void BearerAuthProvider_AddsAuthorizationHeader()
    {
        // Arrange
        var provider = new BearerAuthProvider("my-secret-token");
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.com");

        // Act
        provider.ApplyAuthentication(request, null);

        // Assert
        Assert.NotNull(request.Headers.Authorization);
        Assert.Equal("Bearer", request.Headers.Authorization.Scheme);
        Assert.Equal("my-secret-token", request.Headers.Authorization.Parameter);
    }

    [Fact]
    public void ApiKeyAuthProvider_AddsCustomHeader_DefaultHeaderName()
    {
        // Arrange
        var provider = new ApiKeyAuthProvider("my-api-key");
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.com");

        // Act
        provider.ApplyAuthentication(request, null);

        // Assert
        Assert.True(request.Headers.TryGetValues("X-API-Key", out var values));
        Assert.Equal("my-api-key", values.First());
    }

    [Fact]
    public void ApiKeyAuthProvider_AddsCustomHeader_CustomHeaderName()
    {
        // Arrange
        var provider = new ApiKeyAuthProvider("my-api-key", "Authorization-Key");
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.com");

        // Act
        provider.ApplyAuthentication(request, null);

        // Assert
        Assert.True(request.Headers.TryGetValues("Authorization-Key", out var values));
        Assert.Equal("my-api-key", values.First());
    }

    [Fact]
    public void HmacAuthProvider_AddsSignatureHeader_SHA256()
    {
        // Arrange
        var secret = "my-secret";
        var provider = new HmacAuthProvider(secret);
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.com");
        var body = Encoding.UTF8.GetBytes("{\"message\":\"hello\"}");

        // Act
        provider.ApplyAuthentication(request, body);

        // Assert
        Assert.True(request.Headers.TryGetValues("X-Signature-256", out var values));
        var signature = values.First();
        Assert.StartsWith("sha256=", signature);

        // Verify the signature
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var expectedHash = Convert.ToHexString(hmac.ComputeHash(body)).ToLowerInvariant();
        Assert.Equal($"sha256={expectedHash}", signature);
    }

    [Fact]
    public void HmacAuthProvider_AddsSignatureHeader_SHA1()
    {
        // Arrange
        var secret = "my-secret";
        var provider = new HmacAuthProvider(secret, "X-Hub-Signature", "HMAC-SHA1");
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.com");
        var body = Encoding.UTF8.GetBytes("{\"message\":\"hello\"}");

        // Act
        provider.ApplyAuthentication(request, body);

        // Assert
        Assert.True(request.Headers.TryGetValues("X-Hub-Signature", out var values));
        var signature = values.First();
        Assert.StartsWith("sha1=", signature);
    }

    [Fact]
    public void HmacAuthProvider_AddsSignatureHeader_SHA512()
    {
        // Arrange
        var secret = "my-secret";
        var provider = new HmacAuthProvider(secret, "X-Signature-512", "HMAC-SHA512");
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.com");
        var body = Encoding.UTF8.GetBytes("{\"message\":\"hello\"}");

        // Act
        provider.ApplyAuthentication(request, body);

        // Assert
        Assert.True(request.Headers.TryGetValues("X-Signature-512", out var values));
        var signature = values.First();
        Assert.StartsWith("sha512=", signature);
    }

    [Fact]
    public void HmacAuthProvider_DoesNotAddHeader_WhenBodyIsNull()
    {
        // Arrange
        var provider = new HmacAuthProvider("secret");
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.com");

        // Act
        provider.ApplyAuthentication(request, null);

        // Assert
        Assert.False(request.Headers.Contains("X-Signature-256"));
    }

    [Fact]
    public void HmacAuthProvider_DoesNotAddHeader_WhenBodyIsEmpty()
    {
        // Arrange
        var provider = new HmacAuthProvider("secret");
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.com");

        // Act
        provider.ApplyAuthentication(request, []);

        // Assert
        Assert.False(request.Headers.Contains("X-Signature-256"));
    }

    [Fact]
    public void AuthenticationProviderFactory_CreatesNoAuth_WhenNotSpecified()
    {
        // Arrange
        var config = new Dictionary<string, string>();

        // Act
        var provider = AuthenticationProviderFactory.Create(config);

        // Assert
        Assert.Same(NoAuthProvider.Instance, provider);
    }

    [Fact]
    public void AuthenticationProviderFactory_CreatesBasicAuth()
    {
        // Arrange
        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.AuthType] = HttpConnectorConfig.AuthTypeBasic,
            [HttpConnectorConfig.AuthUsername] = "admin",
            [HttpConnectorConfig.AuthPassword] = "secret"
        };

        // Act
        var provider = AuthenticationProviderFactory.Create(config);
        using var request = new HttpRequestMessage();
        provider.ApplyAuthentication(request, null);

        // Assert
        Assert.IsType<BasicAuthProvider>(provider);
        Assert.NotNull(request.Headers.Authorization);
        Assert.Equal("Basic", request.Headers.Authorization.Scheme);
    }

    [Fact]
    public void AuthenticationProviderFactory_CreatesBearerAuth()
    {
        // Arrange
        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.AuthType] = HttpConnectorConfig.AuthTypeBearer,
            [HttpConnectorConfig.AuthToken] = "my-jwt-token"
        };

        // Act
        var provider = AuthenticationProviderFactory.Create(config);
        using var request = new HttpRequestMessage();
        provider.ApplyAuthentication(request, null);

        // Assert
        Assert.IsType<BearerAuthProvider>(provider);
        Assert.Equal("Bearer", request.Headers.Authorization!.Scheme);
        Assert.Equal("my-jwt-token", request.Headers.Authorization.Parameter);
    }

    [Fact]
    public void AuthenticationProviderFactory_CreatesApiKeyAuth()
    {
        // Arrange
        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.AuthType] = HttpConnectorConfig.AuthTypeApiKey,
            [HttpConnectorConfig.AuthApiKey] = "key-123",
            [HttpConnectorConfig.AuthApiKeyHeader] = "X-Custom-Key"
        };

        // Act
        var provider = AuthenticationProviderFactory.Create(config);
        using var request = new HttpRequestMessage();
        provider.ApplyAuthentication(request, null);

        // Assert
        Assert.IsType<ApiKeyAuthProvider>(provider);
        Assert.True(request.Headers.TryGetValues("X-Custom-Key", out var values));
        Assert.Equal("key-123", values.First());
    }

    [Fact]
    public void AuthenticationProviderFactory_CreatesHmacAuth()
    {
        // Arrange
        var config = new Dictionary<string, string>
        {
            [HttpConnectorConfig.AuthType] = HttpConnectorConfig.AuthTypeHmac,
            [HttpConnectorConfig.AuthHmacSecret] = "webhook-secret",
            [HttpConnectorConfig.AuthHmacHeader] = "X-Hub-Signature-256",
            [HttpConnectorConfig.AuthHmacAlgorithm] = "HMAC-SHA256"
        };

        // Act
        var provider = AuthenticationProviderFactory.Create(config);
        using var request = new HttpRequestMessage();
        var body = "test"u8.ToArray();
        provider.ApplyAuthentication(request, body);

        // Assert
        Assert.IsType<HmacAuthProvider>(provider);
        Assert.True(request.Headers.Contains("X-Hub-Signature-256"));
    }

    [Fact]
    public void SignatureValidator_ValidatesCorrectSignature_SHA256()
    {
        // Arrange
        var secret = "webhook-secret";
        var body = Encoding.UTF8.GetBytes("{\"action\":\"push\"}");

        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var signature = "sha256=" + Convert.ToHexString(hmac.ComputeHash(body)).ToLowerInvariant();

        // Act
        var result = SignatureValidator.ValidateSignature(body, signature, secret);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void SignatureValidator_RejectsIncorrectSignature()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("{\"action\":\"push\"}");
        var signature = "sha256=invalid_signature_value";

        // Act
        var result = SignatureValidator.ValidateSignature(body, signature, "webhook-secret");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void SignatureValidator_RejectsEmptySignature()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("{\"action\":\"push\"}");

        // Act
        var result = SignatureValidator.ValidateSignature(body, "", "webhook-secret");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void SignatureValidator_RejectsEmptySecret()
    {
        // Arrange
        var body = Encoding.UTF8.GetBytes("{\"action\":\"push\"}");

        // Act
        var result = SignatureValidator.ValidateSignature(body, "sha256=something", "");

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void SignatureValidator_ValidatesCorrectSignature_SHA1()
    {
        // Arrange
        var secret = "webhook-secret";
        var body = Encoding.UTF8.GetBytes("{\"action\":\"push\"}");

#pragma warning disable CA5350 // SHA1 used for testing legacy webhook compatibility
        using var hmac = new HMACSHA1(Encoding.UTF8.GetBytes(secret));
#pragma warning restore CA5350
        var signature = "sha1=" + Convert.ToHexString(hmac.ComputeHash(body)).ToLowerInvariant();

        // Act
        var result = SignatureValidator.ValidateSignature(body, signature, secret, "HMAC-SHA1");

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void SignatureValidator_ValidatesCorrectSignature_SHA512()
    {
        // Arrange
        var secret = "webhook-secret";
        var body = Encoding.UTF8.GetBytes("{\"action\":\"push\"}");

        using var hmac = new HMACSHA512(Encoding.UTF8.GetBytes(secret));
        var signature = "sha512=" + Convert.ToHexString(hmac.ComputeHash(body)).ToLowerInvariant();

        // Act
        var result = SignatureValidator.ValidateSignature(body, signature, secret, "HMAC-SHA512");

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void SignatureValidator_ValidatesTimestamp_WithinTolerance()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();

        // Act
        var result = SignatureValidator.ValidateTimestamp(timestamp, 300000);

        // Assert
        Assert.True(result);
    }

    [Fact]
    public void SignatureValidator_RejectsTimestamp_OutsideTolerance()
    {
        // Arrange
        var oldTimestamp = DateTimeOffset.UtcNow.AddMinutes(-10).ToUnixTimeMilliseconds().ToString();

        // Act
        var result = SignatureValidator.ValidateTimestamp(oldTimestamp, 300000); // 5 minutes tolerance

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void SignatureValidator_RejectsTimestamp_InFuture()
    {
        // Arrange
        var futureTimestamp = DateTimeOffset.UtcNow.AddMinutes(10).ToUnixTimeMilliseconds().ToString();

        // Act
        var result = SignatureValidator.ValidateTimestamp(futureTimestamp, 300000);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void SignatureValidator_RejectsTimestamp_WhenNull()
    {
        // Act
        var result = SignatureValidator.ValidateTimestamp(null, 300000);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void SignatureValidator_RejectsTimestamp_WhenEmpty()
    {
        // Act
        var result = SignatureValidator.ValidateTimestamp("", 300000);

        // Assert
        Assert.False(result);
    }

    [Fact]
    public void SignatureValidator_RejectsTimestamp_WhenInvalidFormat()
    {
        // Act
        var result = SignatureValidator.ValidateTimestamp("not-a-number", 300000);

        // Assert
        Assert.False(result);
    }
}
