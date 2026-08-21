using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using XmppDotNet.Xmpp;

namespace Kuestenlogik.Surgewave.Connector.Xmpp.Tests;

/// <summary>
/// The parts of the sink that do not need a live XMPP session: the disconnected-batch guard
/// and the stanza field resolution.
/// </summary>
public class XmppSinkTaskTests
{
    [Fact]
    public async Task PutAsync_WhileDisconnected_FailsTheBatchInsteadOfReportingItAsDelivered()
    {
        var errors = new List<Exception>();

        // A short wait budget; the production default is 50 slices of 100 ms.
        using var task = new XmppSinkTask { ConnectWaitAttempts = 1 };
        task.Initialize(new TaskContext { RaiseError = errors.Add });

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync(
                [Record("""{"to":"friend@example.com","body":"hi"}""")],
                TestContext.Current.CancellationToken));

        // Returning normally here would have the worker commit offsets for messages nobody sent.
        Assert.Same(thrown, Assert.Single(errors));
        Assert.Contains("not established", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void GetString_PrefersThePayloadPropertyOverTheHeader()
    {
        using var document = JsonDocument.Parse("""{"to":"payload@example.com"}""");
        var headers = new Dictionary<string, byte[]>
        {
            ["xmpp.to"] = Encoding.UTF8.GetBytes("header@example.com")
        };

        Assert.Equal("payload@example.com", XmppSinkTask.GetString(document.RootElement, "to", headers));
    }

    [Fact]
    public void GetString_FallsBackToTheXmppPrefixedHeader()
    {
        using var document = JsonDocument.Parse("""{"body":"hi"}""");
        var headers = new Dictionary<string, byte[]>
        {
            ["xmpp.to"] = Encoding.UTF8.GetBytes("header@example.com")
        };

        Assert.Equal("header@example.com", XmppSinkTask.GetString(document.RootElement, "to", headers));
    }

    [Fact]
    public void GetString_WithNeitherPayloadFieldNorHeader_ReturnsNull()
    {
        using var document = JsonDocument.Parse("""{"body":"hi"}""");

        Assert.Null(XmppSinkTask.GetString(document.RootElement, "to", null));
    }

    [Theory]
    [InlineData("groupchat", MessageType.GroupChat)]
    [InlineData("GroupChat", MessageType.GroupChat)]
    [InlineData("normal", MessageType.Normal)]
    [InlineData("headline", MessageType.Headline)]
    [InlineData("chat", MessageType.Chat)]
    [InlineData("whatever", MessageType.Chat)]
    public void ParseMessageType_MapsTheKnownTypesAndFallsBackToChat(string value, MessageType expected)
    {
        Assert.Equal(expected, XmppSinkTask.ParseMessageType(value));
    }

    private static SinkRecord Record(string json) => new()
    {
        Topic = "outbound",
        Partition = 0,
        Offset = 0,
        Value = Encoding.UTF8.GetBytes(json)
    };
}
