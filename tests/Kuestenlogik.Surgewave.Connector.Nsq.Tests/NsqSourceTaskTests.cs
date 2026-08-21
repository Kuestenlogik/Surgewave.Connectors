using System.Globalization;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using NsqSharp;

namespace Kuestenlogik.Surgewave.Connector.Nsq.Tests;

/// <summary>
/// The handler used to <c>TryWrite</c> into a bounded channel and ignore the <c>false</c> result,
/// so every message that arrived while the pipeline was saturated was dropped on the floor. These
/// tests drive the handler-to-poll flow with fake NSQ messages - no nsqd involved - and pin the
/// backpressure, the acknowledgement point and the record shape.
/// </summary>
public class NsqSourceTaskTests
{
    [Fact]
    public async Task PollAsync_TurnsQueuedMessagesIntoRecords()
    {
        using var task = new NsqSourceTask();
        task.Initialize(new TaskContext());
        task.Configure(SourceConfig());

        var message = new FakeMessage("msg-1", Encoding.UTF8.GetBytes("hello"));
        task.HandleMessage(message);

        var record = Assert.Single(await task.PollAsync(CancellationToken.None));

        Assert.Equal("nsq-events", record.Topic);
        Assert.Equal("msg-1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("hello", Encoding.UTF8.GetString(record.Value));
        Assert.Equal("orders", (string)record.SourcePartition["topic"]);
        Assert.Equal("surgewave", (string)record.SourcePartition["channel"]);
        Assert.Equal("msg-1", (string)record.SourceOffset[NsqConnectorConfig.OffsetMessageId]);
        Assert.Equal(message.Timestamp.Ticks, (long)record.SourceOffset[NsqConnectorConfig.OffsetTimestamp]);
        Assert.Equal(new DateTimeOffset(message.Timestamp), record.Timestamp);
        Assert.Equal("orders", Encoding.UTF8.GetString(record.Headers!["nsq.topic"]));
        Assert.Equal("surgewave", Encoding.UTF8.GetString(record.Headers!["nsq.channel"]));
        Assert.Equal("msg-1", Encoding.UTF8.GetString(record.Headers!["nsq.message.id"]));

        // The handler hands acknowledgement over to CommitAsync instead of responding itself.
        Assert.True(message.IsAutoResponseDisabled);
        Assert.False(message.Finished);
    }

    [Fact]
    public async Task PollAsync_StopsAtTheConfiguredBatchSize()
    {
        using var task = new NsqSourceTask();
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[NsqConnectorConfig.BatchSize] = "2";
        task.Configure(config);

        for (var i = 0; i < 5; i++)
        {
            task.HandleMessage(new FakeMessage(Id(i), [1]));
        }

        Assert.Equal(2, (await task.PollAsync(CancellationToken.None)).Count);
        Assert.Equal(2, (await task.PollAsync(CancellationToken.None)).Count);
        Assert.Single(await task.PollAsync(CancellationToken.None));
        Assert.Empty(await task.PollAsync(CancellationToken.None));
    }

    [Fact]
    public async Task CommitAsync_FinishesOnlyTheMessagesThatWerePolled()
    {
        using var task = new NsqSourceTask();
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[NsqConnectorConfig.BatchSize] = "1";
        task.Configure(config);

        var polled = new FakeMessage("msg-1", [1]);
        var stillQueued = new FakeMessage("msg-2", [2]);
        task.HandleMessage(polled);
        task.HandleMessage(stillQueued);

        Assert.Single(await task.PollAsync(CancellationToken.None));
        await task.CommitAsync(CancellationToken.None);

        // Only what actually reached Surgewave may be finished on the NSQ side.
        Assert.True(polled.Finished);
        Assert.False(stillQueued.Finished);
    }

    [Fact]
    public async Task Stop_RequeuesMessagesThatWereNeverCommitted()
    {
        using var task = new NsqSourceTask();
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[NsqConnectorConfig.RequeueDelayMs] = "250";
        task.Configure(config);

        var message = new FakeMessage("msg-1", [1]);
        task.HandleMessage(message);
        Assert.Single(await task.PollAsync(CancellationToken.None));

        task.Stop();

        // A record that was polled but never committed goes back to NSQ instead of vanishing.
        Assert.False(message.Finished);
        Assert.Equal(TimeSpan.FromMilliseconds(250), message.RequeueDelay);
    }

    [Fact]
    public async Task HandleMessage_BlocksInsteadOfDroppingWhenTheQueueIsFull()
    {
        using var task = new NsqSourceTask();
        task.Initialize(new TaskContext());

        var config = SourceConfig();
        config[NsqConnectorConfig.BatchSize] = "1";
        task.Configure(config);

        // The task buffers at most 1000 messages between the NSQ handler and PollAsync.
        for (var i = 0; i < 1000; i++)
        {
            task.HandleMessage(new FakeMessage(Id(i), [1]));
        }

        var overflow = new FakeMessage("overflow", [2]);
        var handoff = Task.Run(() => task.HandleMessage(overflow));

        var finishedEarly = await Task.WhenAny(handoff, Task.Delay(150)) == handoff;

        // NSQ expects the handler to block while the pipeline is saturated. Returning early would
        // drop the message and still let the consumer offsets move past it.
        Assert.False(finishedEarly);
        Assert.False(overflow.HasResponded);

        Assert.Single(await task.PollAsync(CancellationToken.None));
        await handoff.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(handoff.IsCompletedSuccessfully);
    }

    [Fact]
    public void LogFailedMessage_RaisesAnErrorNamingTheMessage()
    {
        using var task = new NsqSourceTask();

        Exception? raised = null;
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });
        task.Configure(SourceConfig());

        task.LogFailedMessage(new FakeMessage("msg-1", [1]) { Attempts = 5 });

        // A message NSQ gave up on is data loss the operator has to hear about.
        var error = Assert.IsType<InvalidOperationException>(raised);
        Assert.Contains("msg-1", error.Message, StringComparison.Ordinal);
        Assert.Contains("orders", error.Message, StringComparison.Ordinal);
        Assert.Contains("5", error.Message, StringComparison.Ordinal);
    }

    private static string Id(int index) => "msg-" + index.ToString(CultureInfo.InvariantCulture);

    private static Dictionary<string, string> SourceConfig() => new(StringComparer.Ordinal)
    {
        [NsqConnectorConfig.NsqdAddress] = "127.0.0.1:4150",
        [NsqConnectorConfig.NsqTopic] = "orders",
        [NsqConnectorConfig.NsqChannel] = "surgewave",
        [NsqConnectorConfig.Topic] = "nsq-events",
        [NsqConnectorConfig.PollTimeoutMs] = "10"
    };

    private sealed class FakeMessage(string id, byte[] body) : IMessage
    {
        public string Id { get; } = id;

        public byte[] Body { get; } = body;

        public DateTime Timestamp { get; } = new DateTime(2026, 8, 20, 12, 0, 0, DateTimeKind.Utc);

        public int Attempts { get; init; } = 1;

        public int MaxAttempts => 5;

        public string NsqdAddress => "127.0.0.1:4150";

        public bool IsAutoResponseDisabled { get; private set; }

        public bool HasResponded { get; private set; }

        public bool BackoffTriggered => false;

        public DateTime? RequeuedUntil { get; private set; }

        public bool Finished { get; private set; }

        public TimeSpan? RequeueDelay { get; private set; }

        public void DisableAutoResponse() => IsAutoResponseDisabled = true;

        public void Finish()
        {
            Finished = true;
            HasResponded = true;
        }

        public void Touch()
        {
        }

        public void Requeue(TimeSpan? delay = null)
        {
            RequeueDelay = delay;
            RequeuedUntil = Timestamp + (delay ?? TimeSpan.Zero);
            HasResponded = true;
        }

        public void RequeueWithoutBackoff(TimeSpan? delay) => Requeue(delay);

        public long WriteTo(Stream writeStream) => Body.Length;
    }
}
