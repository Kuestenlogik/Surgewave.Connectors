using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SignalR.Tests;

/// <summary>
/// Tests for <see cref="SignalRSinkTask"/> around the state the worker can reach without a
/// live hub: a sink that cannot reach its hub has to say so instead of acknowledging records
/// it never sent.
/// </summary>
public class SignalRSinkTaskTests
{
    [Fact]
    public async Task PutAsync_WithoutAConnection_ThrowsInsteadOfDroppingTheRecord()
    {
        using var task = new SignalRSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        // Returning quietly here would let the worker commit the offset for a message the
        // hub never received.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync([Record("hello")], TestContext.Current.CancellationToken));

        Assert.Contains("Not connected", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_WithNothingToSend_DoesNothing()
    {
        using var task = new SignalRSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        // An empty batch must not fail on the missing connection either - the worker calls
        // Put on every poll cycle, connected or not.
        await task.PutAsync([], TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task FlushAsync_WithAnEmptyBuffer_DoesNothing()
    {
        using var task = new SignalRSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        await task.FlushAsync(new Dictionary<TopicPartition, long>(), TestContext.Current.CancellationToken);
    }

    private static SinkRecord Record(string value) => new()
    {
        Topic = "events",
        Partition = 0,
        Offset = 1,
        Key = Encoding.UTF8.GetBytes("user-7"),
        Value = Encoding.UTF8.GetBytes(value),
        Timestamp = DateTimeOffset.UnixEpoch
    };
}
