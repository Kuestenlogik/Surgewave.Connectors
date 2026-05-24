using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Script;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Script.Tests;

/// <summary>
/// Tests for Script transform task.
/// </summary>
public sealed class ScriptTransformTaskTests : IDisposable
{
    private readonly List<ScriptTransformTask> _tasks = [];

    public void Dispose()
    {
        foreach (var task in _tasks)
        {
            try { task.Stop(); } catch { }
            try { task.Dispose(); } catch { }
        }
    }

    private ScriptTransformTask CreateTask()
    {
        var task = new ScriptTransformTask();
        _tasks.Add(task);
        return task;
    }

    [Fact]
    public void ScriptTransformTask_HasCorrectVersion()
    {
        var task = CreateTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void ScriptTransformTask_CompilesInlineScript()
    {
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ScriptInline] = "result.Skip = true;"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void ScriptTransformTask_ThrowsOnInvalidScript()
    {
        var task = CreateTask();
        var context = new TaskContext { RaiseError = _ => { } };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ScriptInline] = "invalid csharp syntax {{{"
        };

        Assert.ThrowsAny<Exception>(() => task.Start(config));
    }

    [Fact]
    public async Task ScriptTransformTask_SkipsRecordWhenSkipIsTrue()
    {
        var task = CreateTask();
        var produced = new List<(string topic, byte[]? key, byte[] value)>();
        var producer = new TestProducer(produced);
        var context = new TaskContext { RaiseError = _ => { }, Producer = producer };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ScriptInline] = "result.Skip = true;"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("test") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(records, cts.Token);

        Assert.Empty(produced);

        task.Stop();
    }

    [Fact]
    public async Task ScriptTransformTask_EmitsTransformedRecord()
    {
        var task = CreateTask();
        var produced = new List<(string topic, byte[]? key, byte[] value)>();
        var producer = new TestProducer(produced);
        var context = new TaskContext { RaiseError = _ => { }, Producer = producer };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ScriptInline] = """
                var upper = ctx.ValueString?.ToUpper() ?? "";
                result.Emit("transformed", upper);
            """
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("hello") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(records, cts.Token);

        Assert.Single(produced);
        Assert.Equal("output", produced[0].topic);
        Assert.Equal("HELLO", Encoding.UTF8.GetString(produced[0].value));

        task.Stop();
    }

    [Fact]
    public async Task ScriptTransformTask_AccessesContextProperties()
    {
        var task = CreateTask();
        var produced = new List<(string topic, byte[]? key, byte[] value)>();
        var producer = new TestProducer(produced);
        var context = new TaskContext { RaiseError = _ => { }, Producer = producer };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ScriptInline] = """
                var info = $"topic={ctx.Topic},partition={ctx.Partition},offset={ctx.Offset}";
                result.Emit(ctx.KeyString, info);
            """
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "input-topic",
                Partition = 5,
                Offset = 42,
                Key = Encoding.UTF8.GetBytes("mykey"),
                Value = Encoding.UTF8.GetBytes("value")
            }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(records, cts.Token);

        Assert.Single(produced);
        var output = Encoding.UTF8.GetString(produced[0].value);
        Assert.Contains("topic=input-topic", output);
        Assert.Contains("partition=5", output);
        Assert.Contains("offset=42", output);

        task.Stop();
    }

    [Fact]
    public async Task ScriptTransformTask_EmitsMultipleRecords()
    {
        var task = CreateTask();
        var produced = new List<(string topic, byte[]? key, byte[] value)>();
        var producer = new TestProducer(produced);
        var context = new TaskContext { RaiseError = _ => { }, Producer = producer };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ScriptInline] = """
                result.Emit("key1", "value1");
                result.Emit("key2", "value2");
                result.Emit("key3", "value3");
            """
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("test") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(records, cts.Token);

        Assert.Equal(3, produced.Count);

        task.Stop();
    }

    [Fact]
    public async Task ScriptTransformTask_EmitWithSameKeyUsesInputKey()
    {
        var task = CreateTask();
        var produced = new List<(string topic, byte[]? key, byte[] value)>();
        var producer = new TestProducer(produced);
        var context = new TaskContext { RaiseError = _ => { }, Producer = producer };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ScriptInline] = """
                result.EmitWithSameKey(System.Text.Encoding.UTF8.GetBytes("transformed"));
            """
        };

        task.Start(config);

        var inputKey = Encoding.UTF8.GetBytes("original-key");
        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Key = inputKey, Value = Encoding.UTF8.GetBytes("value") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(records, cts.Token);

        Assert.Single(produced);
        Assert.Equal("original-key", Encoding.UTF8.GetString(produced[0].key!));

        task.Stop();
    }

    [Fact]
    public async Task ScriptTransformTask_SkipsOnError_WhenConfigured()
    {
        var task = CreateTask();
        var produced = new List<(string topic, byte[]? key, byte[] value)>();
        var producer = new TestProducer(produced);
        var context = new TaskContext { RaiseError = _ => { }, Producer = producer };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ErrorHandling] = "skip",
            [ScriptConnectorConfig.ScriptInline] = """
                throw new System.Exception("Script error");
            """
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("test") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(records, cts.Token);

        // Should skip without producing
        Assert.Empty(produced);

        task.Stop();
    }

    [Fact]
    public async Task ScriptTransformTask_ThrowsOnError_WhenConfiguredToFail()
    {
        var task = CreateTask();
        var produced = new List<(string topic, byte[]? key, byte[] value)>();
        var producer = new TestProducer(produced);
        var context = new TaskContext { RaiseError = _ => { }, Producer = producer };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ErrorHandling] = "fail",
            [ScriptConnectorConfig.ScriptInline] = """
                throw new System.Exception("Script error");
            """
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("test") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await Assert.ThrowsAsync<InvalidOperationException>(() => task.PutAsync(records, cts.Token));

        task.Stop();
    }

    [Fact]
    public async Task ScriptTransformTask_SendsToDeadLetter_WhenConfigured()
    {
        var task = CreateTask();
        var produced = new List<(string topic, byte[]? key, byte[] value)>();
        var producedWithHeaders = new List<(string topic, byte[]? key, byte[] value, IDictionary<string, byte[]>? headers)>();
        var producer = new TestProducerWithHeaders(produced, producedWithHeaders);
        var context = new TaskContext { RaiseError = _ => { }, Producer = producer };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ErrorHandling] = "deadletter",
            [ScriptConnectorConfig.DeadLetterTopic] = "dlq",
            [ScriptConnectorConfig.ScriptInline] = """
                throw new System.Exception("Script error");
            """
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "input", Partition = 0, Offset = 0, Value = Encoding.UTF8.GetBytes("test") }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(records, cts.Token);

        Assert.Single(producedWithHeaders);
        Assert.Equal("dlq", producedWithHeaders[0].topic);
        Assert.NotNull(producedWithHeaders[0].headers);
        Assert.True(producedWithHeaders[0].headers!.ContainsKey("__error"));

        task.Stop();
    }

    [Fact]
    public async Task ScriptTransformTask_HandlesEmptyRecordList()
    {
        var task = CreateTask();
        var produced = new List<(string topic, byte[]? key, byte[] value)>();
        var producer = new TestProducer(produced);
        var context = new TaskContext { RaiseError = _ => { }, Producer = producer };
        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ScriptInline] = "result.Emit(\"key\", \"value\");"
        };

        task.Start(config);

        var records = new List<SinkRecord>();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(records, cts.Token);

        Assert.Empty(produced);

        task.Stop();
    }

    private sealed class TestProducer : IConnectProducer
    {
        private readonly List<(string topic, byte[]? key, byte[] value)> _produced;

        public TestProducer(List<(string topic, byte[]? key, byte[] value)> produced)
        {
            _produced = produced;
        }

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, CancellationToken cancellationToken = default)
        {
            _produced.Add((topic, key, value));
            return Task.CompletedTask;
        }

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, IDictionary<string, byte[]>? headers, CancellationToken cancellationToken = default)
        {
            _produced.Add((topic, key, value));
            return Task.CompletedTask;
        }
    }

    private sealed class TestProducerWithHeaders : IConnectProducer
    {
        private readonly List<(string topic, byte[]? key, byte[] value)> _produced;
        private readonly List<(string topic, byte[]? key, byte[] value, IDictionary<string, byte[]>? headers)> _producedWithHeaders;

        public TestProducerWithHeaders(
            List<(string topic, byte[]? key, byte[] value)> produced,
            List<(string topic, byte[]? key, byte[] value, IDictionary<string, byte[]>? headers)> producedWithHeaders)
        {
            _produced = produced;
            _producedWithHeaders = producedWithHeaders;
        }

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, CancellationToken cancellationToken = default)
        {
            _produced.Add((topic, key, value));
            return Task.CompletedTask;
        }

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, IDictionary<string, byte[]>? headers, CancellationToken cancellationToken = default)
        {
            _producedWithHeaders.Add((topic, key, value, headers));
            return Task.CompletedTask;
        }
    }
}
