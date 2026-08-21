using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Nanomsg.Tests;

/// <summary>
/// The sink used to wrap every send in an empty catch, so a socket that could not deliver a frame
/// dropped the record while the worker committed its offset. A send that cannot happen now has to
/// surface on <see cref="TaskContext.RaiseError"/> and fail the batch.
/// </summary>
public class NanomsgSinkTaskTests
{
    [Fact]
    public async Task PutAsync_RaisesAndThrows_WhenTheSocketCannotSend()
    {
        using var task = new NanomsgSinkTask();

        Exception? raised = null;
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });

        // An unstarted task (or one configured with a receive-only socket) must never report a
        // batch as delivered - the records would be lost behind an advancing offset.
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync([Record("payload")], CancellationToken.None));

        Assert.Same(thrown, raised);
        Assert.Contains("cannot send", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PutAsync_FailsEvenForAnEmptyBatch_WhenTheSocketCannotSend()
    {
        using var task = new NanomsgSinkTask();
        task.Initialize(new TaskContext());

        // The guard runs before the record loop, so an empty batch cannot mask a broken socket.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.PutAsync([], CancellationToken.None));
    }

    private static SinkRecord Record(string value) => new()
    {
        Topic = "nanomsg-out",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(value),
        Timestamp = DateTimeOffset.UnixEpoch
    };
}
