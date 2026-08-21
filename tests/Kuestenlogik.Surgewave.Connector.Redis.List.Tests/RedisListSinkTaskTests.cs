using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Redis.List.Tests;

/// <summary>
/// The sink used to swallow every push failure, which let the worker commit offsets for records
/// that never reached Redis. These tests keep that failure path loud.
/// </summary>
public class RedisListSinkTaskTests
{
    [Fact]
    public async Task PutAsync_SurfacesPushFailures_InsteadOfDroppingTheRecord()
    {
        // The task was never started, so there is no database to push to. Whatever goes wrong,
        // it has to reach the framework and abort the batch - a silent drop would be invisible
        // data loss.
        using var task = new RedisListSinkTask();
        Exception? raised = null;
        task.Initialize(new TaskContext { RaiseError = ex => raised = ex });

        var thrown = await Assert.ThrowsAnyAsync<Exception>(
            () => task.PutAsync([Record("payload")], CancellationToken.None));

        Assert.Same(thrown, raised);
    }

    [Fact]
    public async Task PutAsync_WithNothingToPush_DoesNotTouchRedis()
    {
        // Neither an empty batch nor a tombstone reaches the (missing) connection, so neither
        // may fail.
        using var task = new RedisListSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        await task.PutAsync([], CancellationToken.None);
        await task.PutAsync([RecordWithoutValue()], CancellationToken.None);
    }

    [Fact]
    public void Start_WithoutListKey_FailsBeforeConnecting()
    {
        using var task = new RedisListSinkTask();
        task.Initialize(new TaskContext());

        Assert.Throws<KeyNotFoundException>(() => task.Start(new Dictionary<string, string>()));
    }

    private static SinkRecord Record(string value) => new()
    {
        Topic = "orders",
        Partition = 0,
        Offset = 1,
        Value = Encoding.UTF8.GetBytes(value),
        Timestamp = DateTimeOffset.UnixEpoch
    };

    private static SinkRecord RecordWithoutValue() => new()
    {
        Topic = "orders",
        Partition = 0,
        Offset = 2,
        Value = null!,
        Timestamp = DateTimeOffset.UnixEpoch
    };
}
