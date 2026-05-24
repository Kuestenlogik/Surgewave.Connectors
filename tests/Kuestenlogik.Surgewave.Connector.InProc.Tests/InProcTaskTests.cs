using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.InProc;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.InProc.Tests;

/// <summary>
/// Tests for InProc source and sink tasks using channel mode.
/// </summary>
public sealed class InProcTaskTests : IDisposable
{
    private readonly List<InProcSourceTask> _sourceTasks = [];
    private readonly List<InProcSinkTask> _sinkTasks = [];

    public InProcTaskTests()
    {
        InProcChannel.ClearAll();
    }

    public void Dispose()
    {
        foreach (var task in _sourceTasks)
        {
            try { task.Stop(); } catch { }
            try { task.Dispose(); } catch { }
        }
        foreach (var task in _sinkTasks)
        {
            try { task.Stop(); } catch { }
            try { task.Dispose(); } catch { }
        }
        InProcChannel.ClearAll();
    }

    private InProcSourceTask CreateSourceTask()
    {
        var task = new InProcSourceTask();
        _sourceTasks.Add(task);
        return task;
    }

    private InProcSinkTask CreateSinkTask()
    {
        var task = new InProcSinkTask();
        _sinkTasks.Add(task);
        return task;
    }

    [Fact]
    public void InProcSourceTask_HasCorrectVersion()
    {
        var task = CreateSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void InProcSinkTask_HasCorrectVersion()
    {
        var task = CreateSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task InProc_ChannelMode_SendAndReceive()
    {
        var channelName = "test-channel-" + Guid.NewGuid();

        // Create sink task
        var sinkTask = CreateSinkTask();
        var sinkContext = new TaskContext { RaiseError = _ => { } };
        sinkTask.Initialize(sinkContext);
        sinkTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        // Create source task
        var sourceTask = CreateSourceTask();
        var sourceContext = new TaskContext { RaiseError = _ => { } };
        sourceTask.Initialize(sourceContext);
        sourceTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        // Send a message
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("Hello InProc!") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await sinkTask.PutAsync(records, cts.Token);

        // Small delay to allow message to be written
        await Task.Delay(50);

        // Receive the message
        var received = await sourceTask.PollAsync(cts.Token);

        Assert.Single(received);
        Assert.Equal("Hello InProc!", Encoding.UTF8.GetString(received[0].Value!));
    }

    [Fact]
    public async Task InProc_ChannelMode_SendMultipleMessages()
    {
        var channelName = "multi-msg-" + Guid.NewGuid();

        var sinkTask = CreateSinkTask();
        sinkTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sinkTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        var sourceTask = CreateSourceTask();
        sourceTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sourceTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        // Send multiple messages
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("Message 1") },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = Encoding.UTF8.GetBytes("Message 2") },
            new() { Topic = "test", Partition = 0, Offset = 2, Value = Encoding.UTF8.GetBytes("Message 3") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await sinkTask.PutAsync(records, cts.Token);
        await Task.Delay(50);

        // Receive all messages
        var received = await sourceTask.PollAsync(cts.Token);

        Assert.Equal(3, received.Count);
        Assert.Equal("Message 1", Encoding.UTF8.GetString(received[0].Value!));
        Assert.Equal("Message 2", Encoding.UTF8.GetString(received[1].Value!));
        Assert.Equal("Message 3", Encoding.UTF8.GetString(received[2].Value!));
    }

    [Fact]
    public async Task InProc_ChannelMode_PreservesKeyAndHeaders()
    {
        var channelName = "preserve-" + Guid.NewGuid();

        var sinkTask = CreateSinkTask();
        sinkTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sinkTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        var sourceTask = CreateSourceTask();
        sourceTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sourceTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        var headers = new Dictionary<string, byte[]> { ["custom"] = [1, 2, 3] };
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "test",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("key1"),
                Value = Encoding.UTF8.GetBytes("value1"),
                Headers = headers
            }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await sinkTask.PutAsync(records, cts.Token);
        await Task.Delay(50);

        var received = await sourceTask.PollAsync(cts.Token);

        Assert.Single(received);
        Assert.Equal("key1", Encoding.UTF8.GetString(received[0].Key!));
        Assert.Equal("value1", Encoding.UTF8.GetString(received[0].Value!));
        Assert.Equal([1, 2, 3], received[0].Headers?["custom"]);
    }

    [Fact]
    public async Task InProc_ChannelMode_SkipsNullValues()
    {
        var channelName = "null-values-" + Guid.NewGuid();

        var sinkTask = CreateSinkTask();
        sinkTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sinkTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        var sourceTask = CreateSourceTask();
        sourceTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sourceTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = null! },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = Encoding.UTF8.GetBytes("valid") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await sinkTask.PutAsync(records, cts.Token);
        await Task.Delay(50);

        var received = await sourceTask.PollAsync(cts.Token);

        Assert.Single(received);
        Assert.Equal("valid", Encoding.UTF8.GetString(received[0].Value!));
    }

    [Fact]
    public async Task InProc_SourceTask_ReturnsEmptyWhenNoMessages()
    {
        var channelName = "empty-" + Guid.NewGuid();

        var sourceTask = CreateSourceTask();
        sourceTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sourceTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var received = await sourceTask.PollAsync(cts.Token);

        Assert.Empty(received);
    }

    [Fact]
    public async Task InProc_SinkTask_HandlesEmptyRecordList()
    {
        var channelName = "empty-records-" + Guid.NewGuid();

        var sinkTask = CreateSinkTask();
        sinkTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sinkTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await sinkTask.PutAsync([], cts.Token);

        // Should not throw
    }

    [Fact]
    public void InProc_SourceTask_StopsCleanly()
    {
        var channelName = "stop-source-" + Guid.NewGuid();

        var sourceTask = CreateSourceTask();
        sourceTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sourceTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        sourceTask.Stop();
        // Should not throw
    }

    [Fact]
    public void InProc_SinkTask_StopsCleanly()
    {
        var channelName = "stop-sink-" + Guid.NewGuid();

        var sinkTask = CreateSinkTask();
        sinkTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sinkTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        sinkTask.Stop();
        // Should not throw
    }

    [Fact]
    public async Task InProc_ChannelMode_AssignsIncrementingOffsets()
    {
        var channelName = "offsets-" + Guid.NewGuid();

        var sinkTask = CreateSinkTask();
        sinkTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sinkTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        var sourceTask = CreateSourceTask();
        sourceTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sourceTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test-topic",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName
        });

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = [1] },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = [2] },
            new() { Topic = "test", Partition = 0, Offset = 2, Value = [3] }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await sinkTask.PutAsync(records, cts.Token);
        await Task.Delay(50);

        var received = await sourceTask.PollAsync(cts.Token);

        Assert.Equal(3, received.Count);
        Assert.Equal("test-topic", received[0].Topic);
        Assert.Equal(1L, received[0].SourceOffset?["offset"]);
        Assert.Equal(2L, received[1].SourceOffset?["offset"]);
        Assert.Equal(3L, received[2].SourceOffset?["offset"]);
    }

    [Fact]
    public async Task InProc_ChannelMode_CustomBufferSize()
    {
        var channelName = "custom-buffer-" + Guid.NewGuid();

        var sinkTask = CreateSinkTask();
        sinkTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sinkTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName,
            [InProcConnectorConfig.BufferSize] = "100"
        });

        var sourceTask = CreateSourceTask();
        sourceTask.Initialize(new TaskContext { RaiseError = _ => { } });
        sourceTask.Start(new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = channelName,
            [InProcConnectorConfig.BufferSize] = "100"
        });

        // Send and receive to verify it works
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = [42] }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await sinkTask.PutAsync(records, cts.Token);
        await Task.Delay(50);

        var received = await sourceTask.PollAsync(cts.Token);
        Assert.Single(received);
    }
}
