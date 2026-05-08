using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Nats.Kv;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Nats.Kv.Tests;

/// <summary>
/// Tests for NATS KV source and sink tasks.
/// </summary>
public sealed class NatsKvTaskTests
{
    [Fact]
    public void NatsKvSourceTask_HasCorrectVersion()
    {
        var task = new NatsKvSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void NatsKvSinkTask_HasCorrectVersion()
    {
        var task = new NatsKvSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task NatsKvSinkTask_HandlesEmptyRecords()
    {
        var task = new NatsKvSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        // Task needs NATS connection which requires real server
        // Without Start(), PutAsync should handle empty records gracefully
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await task.PutAsync([], cts.Token);
        // Should not throw
    }

    [Fact]
    public void NatsKvSourceTask_DisposesCleanly()
    {
        var task = new NatsKvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        task.Dispose();
        // Should not throw
    }

    [Fact]
    public void NatsKvSinkTask_DisposesCleanly()
    {
        var task = new NatsKvSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        task.Dispose();
        // Should not throw
    }

    [Fact]
    public void NatsKvSourceTask_StopsCleanly()
    {
        var task = new NatsKvSourceTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        task.Stop();
        // Should not throw
    }

    [Fact]
    public void NatsKvSinkTask_StopsCleanly()
    {
        var task = new NatsKvSinkTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        task.Stop();
        // Should not throw
    }
}
