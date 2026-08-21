using System.Globalization;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using NetMQ;
using NetMQ.Sockets;

namespace Kuestenlogik.Surgewave.Connector.ZeroMQ.Tests;

/// <summary>
/// Drives the sink over NetMQ's in-process transport: real sockets, no network, and above all
/// the guarantee that a send NetMQ refused is never reported as delivered.
/// </summary>
[Collection(NetMqSocketCollection.Name)]
public class ZeroMQSinkTaskTests
{
    /// <summary>How long a test waits for a message the task already handed to NetMQ.</summary>
    private static readonly TimeSpan ReceiveBudget = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData("DEALER")]
    [InlineData("PAIR")]
    public void Start_WithASocketTypeTheConfigDefOffersButTheTaskCannotBuild_ThrowsNamingIt(string socketType)
    {
        using var task = new ZeroMQSinkTask();

        var ex = Assert.Throws<ArgumentException>(() => task.Start(SinkConfig(Endpoint(), socketType)));

        Assert.Contains(socketType, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_WhenTheSendIsRefused_ThrowsAndReportsItInsteadOfDroppingTheRecord()
    {
        var errors = new List<Exception>();
        using var task = new ZeroMQSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });

        // PUSH with nobody attached cannot hand the frame anywhere, so NetMQ's send times out.
        // Ignoring that result is what used to lose the record while the worker committed it.
        task.Start(SinkConfig(Endpoint(), "PUSH", sendTimeoutMs: 300));

        var thrown = await Assert.ThrowsAsync<TimeoutException>(
            () => task.PutAsync([Record("nobody is listening")], TestContext.Current.CancellationToken));

        Assert.Same(thrown, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_PushSocket_DeliversTheRecordValueAsASingleFrame()
    {
        var endpoint = Endpoint();
        using var puller = new PullSocket();
        puller.Bind(endpoint);

        using var task = new ZeroMQSinkTask();
        task.Start(SinkConfig(endpoint, "PUSH", bind: false));

        await task.PutAsync([Record("hello zeromq")], TestContext.Current.CancellationToken);

        Assert.True(puller.TryReceiveFrameBytes(ReceiveBudget, out var frame));
        Assert.Equal("hello zeromq", Encoding.UTF8.GetString(frame!));
    }

    [Fact]
    public async Task PutAsync_PushSocket_DeliversEveryRecordOfTheBatchInOrder()
    {
        var endpoint = Endpoint();
        using var puller = new PullSocket();
        puller.Bind(endpoint);

        using var task = new ZeroMQSinkTask();
        task.Start(SinkConfig(endpoint, "PUSH", bind: false));

        await task.PutAsync(
            [Record("one"), Record("two"), Record("three")],
            TestContext.Current.CancellationToken);

        Assert.Equal("one", ReceiveString(puller));
        Assert.Equal("two", ReceiveString(puller));
        Assert.Equal("three", ReceiveString(puller));
    }

    [Fact]
    public async Task PutAsync_PublisherWithATopicHeader_SendsTheTopicAndThePayloadAsTwoFrames()
    {
        var endpoint = Endpoint();
        using var task = new ZeroMQSinkTask();
        task.Start(SinkConfig(endpoint, "PUB"));

        using var subscriber = new SubscriberSocket();
        subscriber.Connect(endpoint);
        subscriber.SubscribeToAnyTopic();

        // A publisher discards what it sends before the subscription has reached it, so publish
        // until the subscriber sees something or the budget runs out.
        NetMQMessage? published = null;
        for (var attempt = 0; attempt < 20 && published == null; attempt++)
        {
            await task.PutAsync(
                [Record("payload", zeroMqTopic: "alerts")],
                TestContext.Current.CancellationToken);

            if (!subscriber.TryReceiveMultipartMessage(TimeSpan.FromMilliseconds(50), ref published))
            {
                published = null;
            }
        }

        Assert.NotNull(published);
        Assert.Equal(2, published!.FrameCount);
        Assert.Equal("alerts", Encoding.UTF8.GetString(published![0].Buffer));
        Assert.Equal("payload", Encoding.UTF8.GetString(published![1].Buffer));
    }

    [Fact]
    public async Task PutAsync_RequestSocketThatGetsNoReply_FailsTheRecordInsteadOfAssumingDelivery()
    {
        var endpoint = Endpoint();
        using var responder = new ResponseSocket();
        responder.Bind(endpoint);

        using var task = new ZeroMQSinkTask();
        task.Start(SinkConfig(endpoint, "REQ", bind: false, sendTimeoutMs: 1000));

        // The REP peer never answers: the request left the process but nothing confirms it
        // arrived, which the framework has to see as a failed record.
        var ex = await Assert.ThrowsAsync<TimeoutException>(
            () => task.PutAsync([Record("unanswered")], TestContext.Current.CancellationToken));

        Assert.Contains("no reply", ex.Message, StringComparison.Ordinal);
    }

    private static string ReceiveString(PullSocket socket)
    {
        Assert.True(socket.TryReceiveFrameBytes(ReceiveBudget, out var frame));
        return Encoding.UTF8.GetString(frame!);
    }

    /// <summary>A fresh in-process endpoint, so tests never collide on a bound name.</summary>
    private static string Endpoint() => "inproc://" + Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);

    private static SinkRecord Record(string value, string? zeroMqTopic = null) => new()
    {
        Topic = "outbound",
        Partition = 0,
        Offset = 0,
        Value = Encoding.UTF8.GetBytes(value),
        Headers = zeroMqTopic is null
            ? null
            : new Dictionary<string, byte[]> { ["zeromq.topic"] = Encoding.UTF8.GetBytes(zeroMqTopic) }
    };

    private static Dictionary<string, string> SinkConfig(
        string endpoint,
        string socketType,
        bool bind = true,
        int sendTimeoutMs = 5000) => new()
    {
        [ZeroMQConnectorConfig.Topics] = "outbound",
        [ZeroMQConnectorConfig.Endpoints] = endpoint,
        [ZeroMQConnectorConfig.SocketType] = socketType,
        [ZeroMQConnectorConfig.BindMode] = bind ? "true" : "false",
        [ZeroMQConnectorConfig.LingerMs] = "0",
        [ZeroMQConnectorConfig.SendTimeoutMs] = sendTimeoutMs.ToString(CultureInfo.InvariantCulture)
    };
}
