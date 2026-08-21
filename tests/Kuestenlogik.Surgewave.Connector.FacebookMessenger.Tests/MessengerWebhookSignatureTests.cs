using System.Security.Cryptography;
using System.Text;

namespace Kuestenlogik.Surgewave.Connector.FacebookMessenger.Tests;

/// <summary>
/// Tests for the X-Hub-Signature-256 check of the Messenger webhook. The listener publishes
/// whatever it accepts straight into a topic, so a configured app secret has to reject every
/// delivery it cannot verify.
/// </summary>
public class MessengerWebhookSignatureTests
{
    private static readonly byte[] AppSecret = Encoding.UTF8.GetBytes("app-secret");
    private static readonly byte[] Body = Encoding.UTF8.GetBytes("""{"object":"page","entry":[]}""");

    [Fact]
    public void WithoutAnAppSecret_VerificationIsSkipped()
    {
        // Verification is opt-in: an unconfigured secret leaves nothing to verify against.
        Assert.True(MessengerSourceTask.IsSignatureValid(secret: null, header: null, Body));
    }

    [Fact]
    public void AcceptsADeliverySignedWithTheAppSecret()
    {
        Assert.True(MessengerSourceTask.IsSignatureValid(AppSecret, Sign(AppSecret, Body), Body));
    }

    [Fact]
    public void RejectsADeliverySignedWithAnotherSecret()
    {
        var forged = Sign(Encoding.UTF8.GetBytes("not-the-app-secret"), Body);

        Assert.False(MessengerSourceTask.IsSignatureValid(AppSecret, forged, Body));
    }

    [Fact]
    public void RejectsADeliveryWhoseBodyWasTamperedWith()
    {
        var header = Sign(AppSecret, Body);
        var tampered = Encoding.UTF8.GetBytes("""{"object":"page","entry":[{"id":"injected"}]}""");

        Assert.False(MessengerSourceTask.IsSignatureValid(AppSecret, header, tampered));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("deadbeef")]
    [InlineData("sha1=deadbeef")]
    [InlineData("sha256=not-hexadecimal")]
    [InlineData("sha256=")]
    public void RejectsAMissingOrMalformedSignatureHeader(string? header)
    {
        Assert.False(MessengerSourceTask.IsSignatureValid(AppSecret, header, Body));
    }

    private static string Sign(byte[] secret, byte[] body) =>
        "sha256=" + Convert.ToHexString(HMACSHA256.HashData(secret, body));
}
