using System.Text;

namespace Kuestenlogik.Surgewave.Connector.SignalR.Tests;

/// <summary>
/// Tests for <see cref="SignalRSourceTask"/>. The hub callback and the poll loop meet in a
/// bounded buffer, so these cover the record a hub message turns into and what happens when
/// the buffer runs full - the point where messages used to disappear without a trace.
/// </summary>
public class SignalRSourceTaskTests
{
    private const string HubUrl = "http://hub.invalid/events";

    /// <summary>Capacity of the task's bounded record buffer.</summary>
    private const int BufferCapacity = 10000;

    /// <summary>Maximum number of records a single poll hands back.</summary>
    private const int PollBatchSize = 1000;

    [Theory]
    [InlineData(SignalRConfig.HubUrl)]
    [InlineData(SignalRConfig.Topic)]
    public void Start_WithoutARequiredKey_FailsBeforeConnecting(string missingKey)
    {
        using var task = new SignalRSourceTask();

        var config = SourceConfig();
        config.Remove(missingKey);

        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
        Assert.Contains(missingKey, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Start_WithAMessageShapeTheClientCannotBind_FailsBeforeConnecting()
    {
        using var task = new SignalRSourceTask();

        var config = SourceConfig();
        config[SignalRConfig.MessageFormat] = "protobuf";

        // The task used to register three handlers for the same hub method, one per shape.
        // The SignalR client binds a hub method against a single signature, so the extra
        // registrations dropped or duplicated messages. Now exactly one shape is wired up,
        // which means an unknown shape has to be rejected rather than silently ignored.
        var ex = Assert.Throws<ArgumentException>(() => task.Start(config));

        Assert.Contains("key-value, value-only, json", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PollAsync_HandsBackWhatTheHubCallbackEnqueued()
    {
        using var task = new SignalRSourceTask();

        await task.EnqueueAsync(HubUrl, "user-7", "hello");
        await task.EnqueueAsync(HubUrl, null, "world");

        var records = await task.PollAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, records.Count);
        Assert.Equal("user-7", Encoding.UTF8.GetString(records[0].Key!));
        Assert.Equal("hello", Encoding.UTF8.GetString(records[0].Value));

        // The hub and the method are the source partition, so offsets survive a restart
        // even when one worker serves several hubs.
        Assert.Equal(HubUrl, Assert.IsType<string>(records[0].SourcePartition["hub"]));
        Assert.Equal(SignalRConfig.DefaultMethod, Assert.IsType<string>(records[0].SourcePartition["method"]));
        Assert.Equal(1L, Assert.IsType<long>(records[0].SourceOffset["messageId"]));

        // A value-only message carries no key at all; an empty key would pin every message
        // of the hub onto one partition.
        Assert.Null(records[1].Key);
        Assert.Equal("world", Encoding.UTF8.GetString(records[1].Value));
        Assert.Equal(2L, Assert.IsType<long>(records[1].SourceOffset["messageId"]));
    }

    [Fact]
    public async Task EnqueueAsync_WaitsForRoomInsteadOfDroppingMessages()
    {
        using var task = new SignalRSourceTask();

        for (var i = 0; i < BufferCapacity; i++)
        {
            await task.EnqueueAsync(HubUrl, null, "m");
        }

        var overflow = task.EnqueueAsync(HubUrl, null, "overflow");
        await Task.Delay(50, TestContext.Current.CancellationToken);

        // TryWrite would have returned false right here and thrown the message away -
        // BoundedChannelFullMode.Wait only ever applies to WriteAsync. Backpressure on the
        // hub callback is the whole point: the message waits, it does not vanish.
        Assert.False(overflow.IsCompleted);

        var firstBatch = await task.PollAsync(TestContext.Current.CancellationToken);
        Assert.Equal(PollBatchSize, firstBatch.Count);

        await overflow;

        var delivered = firstBatch.Count;
        while (true)
        {
            var batch = await task.PollAsync(TestContext.Current.CancellationToken);
            if (batch.Count == 0)
            {
                break;
            }

            delivered += batch.Count;
        }

        Assert.Equal(BufferCapacity + 1, delivered);
    }

    [Fact]
    public async Task PollAsync_WithNothingBuffered_HandsBackNoRecords()
    {
        using var task = new SignalRSourceTask();

        Assert.Empty(await task.PollAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task EnqueueAsync_AfterStop_DoesNotTearTheTaskDown()
    {
        using var task = new SignalRSourceTask();
        task.Stop();

        // Stop closes the buffer while hub callbacks may still be in flight; the last one
        // has nowhere to go, but it must not escalate into a task failure.
        await task.EnqueueAsync(HubUrl, null, "late");

        Assert.Empty(await task.PollAsync(TestContext.Current.CancellationToken));
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [SignalRConfig.HubUrl] = HubUrl,
        [SignalRConfig.Topic] = "signalr-events"
    };
}
