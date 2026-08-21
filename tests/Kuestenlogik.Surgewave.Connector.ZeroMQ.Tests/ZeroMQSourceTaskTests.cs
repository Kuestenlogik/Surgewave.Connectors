using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using NetMQ;
using NetMQ.Sockets;

namespace Kuestenlogik.Surgewave.Connector.ZeroMQ.Tests;

/// <summary>
/// Drives the source over NetMQ's in-process transport: real frames arrive on a real socket, so
/// the record mapping and the format handling are exercised without a network.
/// </summary>
[Collection(NetMqSocketCollection.Name)]
public class ZeroMQSourceTaskTests
{
    /// <summary>How long a test's own socket waits for the task's socket to attach.</summary>
    private static readonly TimeSpan SendBudget = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData("DEALER")]
    [InlineData("PAIR")]
    public void Start_WithASocketTypeTheConfigDefOffersButTheTaskCannotBuild_ThrowsNamingIt(string socketType)
    {
        using var task = new ZeroMQSourceTask();

        var ex = Assert.Throws<ArgumentException>(() => task.Start(SourceConfig(Endpoint(), socketType)));

        Assert.Contains(socketType, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_PullSocket_TurnsEveryFrameIntoItsOwnNumberedRecord()
    {
        var endpoint = Endpoint();
        using var pusher = new PushSocket();
        pusher.Bind(endpoint);

        using var task = new ZeroMQSourceTask();
        task.Start(SourceConfig(endpoint, "PULL"));

        Assert.True(pusher.TrySendFrame(SendBudget, Encoding.UTF8.GetBytes("first")));
        Assert.True(pusher.TrySendFrame(SendBudget, Encoding.UTF8.GetBytes("second")));

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, records.Count);
        Assert.Equal("zeromq-in", records[0].Topic);
        Assert.Equal("first", Encoding.UTF8.GetString(records[0].Value));
        Assert.Equal("second", Encoding.UTF8.GetString(records[1].Value));
        Assert.Equal("1", Encoding.UTF8.GetString(records[0].Key!));
        Assert.Equal("2", Encoding.UTF8.GetString(records[1].Key!));
        Assert.Equal(1L, records[0].SourceOffset["message_id"]);
        Assert.Equal(2L, records[1].SourceOffset["message_id"]);
        Assert.Equal("zeromq", records[0].SourcePartition["source"]);
        Assert.Equal("PULL", records[0].SourcePartition["socket"]);
        Assert.Equal("PULL", HeaderValue(records[0], "zeromq.socket.type"));
        Assert.Equal("1", HeaderValue(records[0], "zeromq.frame.count"));
    }

    [Fact]
    public async Task PollAsync_MultipartFormat_KeepsTheFirstFrameAsKeyAndBase64EncodesTheRest()
    {
        var endpoint = Endpoint();
        using var pusher = new PushSocket();
        pusher.Bind(endpoint);

        using var task = new ZeroMQSourceTask();
        task.Start(SourceConfig(endpoint, "PULL", format: "multipart"));

        var message = new NetMQMessage();
        message.Append("routing-key");
        message.Append(Encoding.UTF8.GetBytes("payload"));
        Assert.True(pusher.TrySendMultipartMessage(SendBudget, message));

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        Assert.Equal("routing-key", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("2", HeaderValue(record, "zeromq.frame.count"));

        using var document = JsonDocument.Parse(record.Value);
        var frames = document.RootElement.GetProperty("frames");
        Assert.Equal(1, frames.GetArrayLength());
        Assert.Equal(Convert.ToBase64String(Encoding.UTF8.GetBytes("payload")), frames[0].GetString());
    }

    [Fact]
    public async Task PollAsync_JsonFormat_WrapsThePayloadAsBase64SoBinaryStaysIntact()
    {
        var endpoint = Endpoint();
        using var pusher = new PushSocket();
        pusher.Bind(endpoint);

        using var task = new ZeroMQSourceTask();
        task.Start(SourceConfig(endpoint, "PULL", format: "json"));

        var payload = new byte[] { 0x00, 0xFF, 0x10 };
        Assert.True(pusher.TrySendFrame(SendBudget, payload));

        var record = Assert.Single(await task.PollAsync(TestContext.Current.CancellationToken));

        using var document = JsonDocument.Parse(record.Value);
        Assert.Equal("base64", document.RootElement.GetProperty("encoding").GetString());
        Assert.Equal(Convert.ToBase64String(payload), document.RootElement.GetProperty("data").GetString());
    }

    [Fact]
    public async Task PollAsync_WithNothingQueued_ReturnsEmptyOnceTheReceiveTimeoutElapses()
    {
        var endpoint = Endpoint();
        using var pusher = new PushSocket();
        pusher.Bind(endpoint);

        using var task = new ZeroMQSourceTask();
        task.Start(SourceConfig(endpoint, "PULL"));

        Assert.Empty(await task.PollAsync(TestContext.Current.CancellationToken));
    }

    private static string HeaderValue(SourceRecord record, string name) =>
        Encoding.UTF8.GetString(record.Headers![name]);

    /// <summary>A fresh in-process endpoint, so tests never collide on a bound name.</summary>
    private static string Endpoint() => "inproc://" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);

    private static Dictionary<string, string> SourceConfig(
        string endpoint,
        string socketType,
        string format = "raw") => new()
    {
        [ZeroMQConnectorConfig.Topic] = "zeromq-in",
        [ZeroMQConnectorConfig.Endpoints] = endpoint,
        [ZeroMQConnectorConfig.SocketType] = socketType,
        [ZeroMQConnectorConfig.BindMode] = "false",
        [ZeroMQConnectorConfig.LingerMs] = "0",
        [ZeroMQConnectorConfig.MessageFormat] = format,
        [ZeroMQConnectorConfig.ReceiveTimeoutMs] = "200"
    };
}
