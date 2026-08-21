using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using XmppDotNet.Xmpp;
using XmppDotNet.Xmpp.Client;

namespace Kuestenlogik.Surgewave.Connector.Xmpp.Tests;

/// <summary>
/// Hands stanzas to the source task directly - no server, no socket - and checks the filtering
/// and the record mapping that a live session would otherwise hide.
/// </summary>
public class XmppSourceTaskTests
{
    [Fact]
    public async Task PollAsync_AfterAChatMessage_ProducesARecordKeyedByTheSenderJid()
    {
        using var task = new XmppSourceTask();
        task.ApplyConfig(SourceConfig());

        task.OnMessageReceived(ChatMessage("friend@example.com", "hi there"));

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal("inbound", record.Topic);
        Assert.Equal("friend@example.com", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("message", HeaderValue(record, "xmpp.type"));
        Assert.Equal("chat", HeaderValue(record, "xmpp.message.type"));
        Assert.Equal("friend@example.com", HeaderValue(record, "xmpp.from"));
        Assert.Equal("xmpp", record.SourcePartition["source"]);
        Assert.Equal("message", record.SourcePartition["type"]);
        Assert.Equal(1L, record.SourceOffset["message_id"]);
        Assert.Equal("stanza-1", record.SourceOffset["xmpp_id"]);

        using var document = JsonDocument.Parse(record.Value);
        Assert.Equal("hi there", document.RootElement.GetProperty("body").GetString());
        Assert.Equal("friend@example.com", document.RootElement.GetProperty("fromBare").GetString());
        Assert.Equal("chat", document.RootElement.GetProperty("messageType").GetString());
    }

    [Fact]
    public async Task OnMessageReceived_WithGroupChatTurnedOff_DropsMucTrafficButKeepsDirectChats()
    {
        using var task = new XmppSourceTask();
        var config = SourceConfig();
        config[XmppConnectorConfig.IncludeGroupChat] = "false";
        task.ApplyConfig(config);

        task.OnMessageReceived(GroupChatMessage("room@conference.example.com", "hello room"));
        task.OnMessageReceived(ChatMessage("friend@example.com", "direct"));

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal("friend@example.com", Encoding.UTF8.GetString(record.Key!));
    }

    [Fact]
    public async Task OnMessageReceived_WithGroupChatLeftOn_KeepsMucTrafficAndLabelsItAsGroupChat()
    {
        using var task = new XmppSourceTask();
        task.ApplyConfig(SourceConfig());

        task.OnMessageReceived(GroupChatMessage("room@conference.example.com", "hello room"));

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal("groupchat", HeaderValue(record, "xmpp.message.type"));
        Assert.Equal("room@conference.example.com", Encoding.UTF8.GetString(record.Key!));
    }

    [Fact]
    public async Task OnMessageReceived_WithAJidFilter_KeepsOnlyTheListedContacts()
    {
        using var task = new XmppSourceTask();
        var config = SourceConfig();

        // Spelled differently on purpose: JIDs are compared case-insensitively.
        config[XmppConnectorConfig.FilterJids] = "Friend@Example.com, other@example.com";
        task.ApplyConfig(config);

        task.OnMessageReceived(ChatMessage("stranger@example.com", "spam"));
        task.OnMessageReceived(ChatMessage("friend@example.com", "wanted"));

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        using var document = JsonDocument.Parse(record.Value);
        Assert.Equal("wanted", document.RootElement.GetProperty("body").GetString());
    }

    [Fact]
    public async Task PollAsync_DrainsTheQueueAndNumbersEveryRecordItHandsOut()
    {
        using var task = new XmppSourceTask();
        task.ApplyConfig(SourceConfig());

        task.OnMessageReceived(ChatMessage("a@example.com", "first"));
        task.OnMessageReceived(ChatMessage("b@example.com", "second"));

        var records = await task.PollAsync(TestContext.Current.CancellationToken);
        var afterDrain = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, records.Count);
        Assert.Equal(1L, records[0].SourceOffset["message_id"]);
        Assert.Equal(2L, records[1].SourceOffset["message_id"]);
        Assert.Empty(afterDrain);
    }

    [Fact]
    public async Task PollAsync_WithoutAnyStanza_ReturnsEmptyInsteadOfBlocking()
    {
        using var task = new XmppSourceTask();
        task.ApplyConfig(SourceConfig());

        Assert.Empty(await task.PollAsync(TestContext.Current.CancellationToken));
    }

    private static string HeaderValue(SourceRecord record, string name) =>
        Encoding.UTF8.GetString(record.Headers![name]);

    private static Message ChatMessage(string from, string body) => new()
    {
        From = from,
        Type = MessageType.Chat,
        Body = body,
        Id = "stanza-1"
    };

    private static Message GroupChatMessage(string from, string body) => new()
    {
        From = from,
        Type = MessageType.GroupChat,
        Body = body,
        Id = "stanza-2"
    };

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [XmppConnectorConfig.Topic] = "inbound",
        [XmppConnectorConfig.Host] = "xmpp.example.com",
        [XmppConnectorConfig.Domain] = "example.com",
        [XmppConnectorConfig.Username] = "bot",
        [XmppConnectorConfig.Password] = "secret"
    };
}
