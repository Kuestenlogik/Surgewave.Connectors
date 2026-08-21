using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Logic.Tests;

/// <summary>
/// Fan-out rules of the split node: array elements become individual records, anything else is
/// passed through untouched.
/// </summary>
public class SplitTaskTests
{
    [Fact]
    public async Task PutAsync_SplitsRootArrayIntoOneRecordPerElement()
    {
        var producer = new RecordingProducer();
        using var task = new SplitTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("."));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""[{"id":1},{"id":2},{"id":3}]""")], cts.Token);

        Assert.Equal(3, producer.Produced.Count);
        Assert.Equal("""{"id":1}""", Encoding.UTF8.GetString(producer.Produced[0].Value));
        Assert.Equal("""{"id":3}""", Encoding.UTF8.GetString(producer.Produced[2].Value));
        Assert.All(producer.Produced, p => Assert.Equal("split-output", p.Topic));
    }

    [Fact]
    public async Task PutAsync_SplitsArrayBehindANestedPath()
    {
        var producer = new RecordingProducer();
        using var task = new SplitTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("data.records"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"data":{"records":[{"id":1},{"id":2}]}}""")], cts.Token);

        Assert.Equal(2, producer.Produced.Count);
        Assert.Equal("""{"id":1}""", Encoding.UTF8.GetString(producer.Produced[0].Value));
    }

    [Fact]
    public async Task PutAsync_AddsTheElementIndexHeader_WhenEnabled()
    {
        var producer = new RecordingProducer();
        using var task = new SplitTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });

        var config = Config(".");
        config[SplitConfig.IncludeIndex] = "true";
        task.Start(config);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""["a","b"]""")], cts.Token);

        Assert.Equal(0, BitConverter.ToInt32(producer.Produced[0].Headers!["_index"]));
        Assert.Equal(1, BitConverter.ToInt32(producer.Produced[1].Headers!["_index"]));
    }

    [Fact]
    public async Task PutAsync_PassesNonArrayPayloadsThrough()
    {
        var producer = new RecordingProducer();
        using var task = new SplitTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("."));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"id":1}""")], cts.Token);

        Assert.Equal("""{"id":1}""", Encoding.UTF8.GetString(Assert.Single(producer.Produced).Value));
    }

    [Fact]
    public async Task PutAsync_CopiesRecordHeadersOntoEveryElement()
    {
        var producer = new RecordingProducer();
        using var task = new SplitTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("."));

        var record = new SinkRecord
        {
            Topic = "input",
            Partition = 0,
            Offset = 0,
            Key = Encoding.UTF8.GetBytes("batch-1"),
            Value = Encoding.UTF8.GetBytes("""[1,2]"""),
            Headers = new Dictionary<string, byte[]>
            {
                ["trace-id"] = Encoding.UTF8.GetBytes("abc")
            }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([record], cts.Token);

        Assert.Equal(2, producer.Produced.Count);
        Assert.All(producer.Produced, p =>
        {
            Assert.Equal("batch-1", Encoding.UTF8.GetString(p.Key!));
            Assert.Equal("abc", Encoding.UTF8.GetString(p.Headers!["trace-id"]));
        });
    }

    private static Dictionary<string, string> Config(string arrayPath) => new()
    {
        [SplitConfig.ArrayPath] = arrayPath,
        [SplitConfig.OutputTopic] = "split-output",
        [SplitConfig.IncludeIndex] = "false"
    };

    private static SinkRecord Record(string json) => new()
    {
        Topic = "input",
        Partition = 0,
        Offset = 0,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private sealed class RecordingProducer : IConnectProducer
    {
        public List<(string Topic, byte[]? Key, byte[] Value, IDictionary<string, byte[]>? Headers)> Produced { get; } = [];

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, CancellationToken cancellationToken = default)
            => ProduceAsync(topic, key, value, null, cancellationToken);

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, IDictionary<string, byte[]>? headers,
            CancellationToken cancellationToken = default)
        {
            Produced.Add((topic, key, value, headers));
            return Task.CompletedTask;
        }
    }
}
