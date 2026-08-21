using System.Security.Cryptography;
using System.Text;

namespace Kuestenlogik.Surgewave.Connector.Instagram.Tests;

/// <summary>
/// Webhook authenticity checks of the Instagram source. Meta signs every delivery with
/// <c>X-Hub-Signature-256</c>; an unsigned or tampered POST must not become a "genuine" event.
/// </summary>
public class InstagramSourceTaskTests
{
    private const string Secret = "app-secret";

    private const string Body =
        """{"object":"instagram","entry":[{"id":"17841400000000000","changes":[{"field":"comments"}]}]}""";

    [Fact]
    public void IsSignatureValid_WithoutConfiguredAppSecret_AcceptsUnsignedRequests()
    {
        Assert.True(InstagramSourceTask.IsSignatureValid(null, Body, null));
    }

    [Fact]
    public void IsSignatureValid_AcceptsCorrectlySignedBody()
    {
        Assert.True(InstagramSourceTask.IsSignatureValid(SecretBytes(), Body, Sign(Body)));
    }

    [Fact]
    public void IsSignatureValid_RejectsTamperedBody()
    {
        var signature = Sign(Body);
        var tampered = Body.Replace("comments", "mentions", StringComparison.Ordinal);

        Assert.False(InstagramSourceTask.IsSignatureValid(SecretBytes(), tampered, signature));
    }

    [Fact]
    public void IsSignatureValid_RejectsRequestWithoutSignatureHeader()
    {
        Assert.False(InstagramSourceTask.IsSignatureValid(SecretBytes(), Body, null));
    }

    [Fact]
    public void IsSignatureValid_RejectsSignatureWithoutAlgorithmPrefix()
    {
        var hex = Convert.ToHexString(HMACSHA256.HashData(SecretBytes(), Encoding.UTF8.GetBytes(Body)));

        Assert.False(InstagramSourceTask.IsSignatureValid(SecretBytes(), Body, hex));
    }

    [Fact]
    public void IsSignatureValid_RejectsMalformedHexSignature()
    {
        Assert.False(InstagramSourceTask.IsSignatureValid(SecretBytes(), Body, "sha256=nothexadecimal"));
    }

    [Fact]
    public void IsSignatureValid_AcceptsUppercasePrefix()
    {
        var signature = Sign(Body).Replace("sha256=", "SHA256=", StringComparison.Ordinal);

        Assert.True(InstagramSourceTask.IsSignatureValid(SecretBytes(), Body, signature));
    }

    private static byte[] SecretBytes() => Encoding.UTF8.GetBytes(Secret);

    private static string Sign(string body) =>
        "sha256=" + Convert.ToHexString(HMACSHA256.HashData(SecretBytes(), Encoding.UTF8.GetBytes(body)));
}
