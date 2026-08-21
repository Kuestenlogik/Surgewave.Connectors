using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Discord.Tests;

/// <summary>
/// Tests for the gateway-to-poll handover of <see cref="DiscordSourceTask"/>. The queue is
/// filled asynchronously by gateway events, so the only thing PollAsync may do is hand out
/// at most one batch - dropping the record that overflows the batch loses a Discord message
/// for good, because nothing ever redelivers it.
/// </summary>
public class DiscordSourceTaskTests
{
    private const int BatchSize = 100;

    [Fact]
    public async Task PollAsync_WithNothingQueued_ReturnsNoRecords()
    {
        using var task = new DiscordSourceTask();

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Empty(records);
    }

    [Fact]
    public async Task PollAsync_ReturnsQueuedRecordsInGatewayOrder()
    {
        using var task = new DiscordSourceTask();
        task.EnqueueRecord(Record(1));
        task.EnqueueRecord(Record(2));
        task.EnqueueRecord(Record(3));

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(new[] { 1, 2, 3 }, records.Select(MessageId));
    }

    [Fact]
    public async Task PollAsync_HandsOutAtMostOneBatch()
    {
        using var task = new DiscordSourceTask();
        Enqueue(task, count: 150);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(BatchSize, records.Count);
        Assert.Equal(1, MessageId(records[0]));
        Assert.Equal(BatchSize, MessageId(records[^1]));
    }

    [Fact]
    public async Task PollAsync_KeepsTheRecordThatOverflowsTheBatch()
    {
        // The record beyond the batch limit must stay queued: a dequeue-then-drop would
        // acknowledge it without ever emitting it.
        using var task = new DiscordSourceTask();
        Enqueue(task, count: BatchSize + 1);

        var first = await task.PollAsync(CancellationToken.None);
        var second = await task.PollAsync(CancellationToken.None);

        Assert.Equal(BatchSize, first.Count);
        var overflow = Assert.Single(second);
        Assert.Equal(BatchSize + 1, MessageId(overflow));
    }

    [Fact]
    public async Task PollAsync_EventuallyDrainsEveryQueuedRecord()
    {
        using var task = new DiscordSourceTask();
        Enqueue(task, count: 250);

        var polled = new List<int>();
        for (var i = 0; i < 4; i++)
        {
            polled.AddRange((await task.PollAsync(CancellationToken.None)).Select(MessageId));
        }

        Assert.Equal(Enumerable.Range(1, 250), polled);
    }

    [Fact]
    public void Dispose_WithoutStart_DoesNotThrow()
    {
        var task = new DiscordSourceTask();

        task.Dispose();
    }

    private static void Enqueue(DiscordSourceTask task, int count)
    {
        for (var i = 1; i <= count; i++)
        {
            task.EnqueueRecord(Record(i));
        }
    }

    private static int MessageId(SourceRecord record) => Assert.IsType<int>(record.SourceOffset["message_id"]);

    private static SourceRecord Record(int messageId) => new()
    {
        SourcePartition = new Dictionary<string, object> { ["source"] = "discord" },
        SourceOffset = new Dictionary<string, object> { ["message_id"] = messageId },
        Topic = "discord-events",
        Value = Encoding.UTF8.GetBytes("payload")
    };
}
