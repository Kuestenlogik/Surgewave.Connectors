using System.Text;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Logic.Tests;

/// <summary>
/// Windowing behaviour of the aggregate node. The window is only allowed to disappear once its
/// aggregates have actually been produced - a failing producer must not silently drop them.
/// </summary>
public class AggregateTaskTests
{
    [Fact]
    public async Task FlushAsync_ProducesOneAggregatePerGroup()
    {
        var producer = new RecordingProducer();
        using var task = new AggregateTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("count", groupBy: "region"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync(
        [
            Record("""{"region":"north"}"""),
            Record("""{"region":"north"}""", 1),
            Record("""{"region":"south"}""", 2)
        ], cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        Assert.Equal(2, producer.Produced.Count);

        var north = producer.Produced.Single(p => Encoding.UTF8.GetString(p.Key!) == "north");
        Assert.Equal("aggregates", north.Topic);

        var payload = Payload(north.Value);
        Assert.Equal("north", payload["group"]!.GetValue<string>());
        Assert.Equal("count", payload["operation"]!.GetValue<string>());
        Assert.Equal(2, payload["count"]!.GetValue<int>());
        Assert.Equal(2d, payload["result"]!.GetValue<double>());
    }

    [Fact]
    public async Task FlushAsync_ClosesTheWindow()
    {
        var producer = new RecordingProducer();
        using var task = new AggregateTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("count"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("{}")], cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);
        producer.Produced.Clear();

        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        Assert.Empty(producer.Produced);
    }

    [Fact]
    public async Task FlushAsync_KeepsTheWindow_WhenProducingFails()
    {
        var producer = new RecordingProducer { Fail = true };
        using var task = new AggregateTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("count"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("{}"), Record("{}", 1), Record("{}", 2)], cts.Token);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token));

        producer.Fail = false;
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        var produced = Assert.Single(producer.Produced);
        Assert.Equal(3, Payload(produced.Value)["count"]!.GetValue<int>());
    }

    [Fact]
    public async Task FlushAsync_SumsTheConfiguredField()
    {
        var producer = new RecordingProducer();
        using var task = new AggregateTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("sum", field: "amount"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"amount":10}"""), Record("""{"amount":5}""", 1)], cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        Assert.Equal(15d, Payload(Assert.Single(producer.Produced).Value)["result"]!.GetValue<double>());
    }

    [Fact]
    public async Task FlushAsync_AveragesTheConfiguredField()
    {
        var producer = new RecordingProducer();
        using var task = new AggregateTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("avg", field: "amount"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"amount":10}"""), Record("""{"amount":5}""", 1)], cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        Assert.Equal(7.5d, Payload(Assert.Single(producer.Produced).Value)["result"]!.GetValue<double>());
    }

    [Fact]
    public async Task PutAsync_GroupsByNestedField()
    {
        var producer = new RecordingProducer();
        using var task = new AggregateTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("count", groupBy: "user.country"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"user":{"country":"de"}}""")], cts.Token);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), cts.Token);

        var produced = Assert.Single(producer.Produced);
        Assert.Equal("de", Encoding.UTF8.GetString(produced.Key!));
    }

    private static JsonNode Payload(byte[] value) => JsonNode.Parse(Encoding.UTF8.GetString(value))!;

    private static Dictionary<string, string> Config(string operation, string groupBy = "", string field = "") => new()
    {
        [AggregateConfig.Operation] = operation,
        [AggregateConfig.OutputTopic] = "aggregates",
        [AggregateConfig.WindowMs] = "600000",
        [AggregateConfig.GroupByField] = groupBy,
        [AggregateConfig.AggregateField] = field
    };

    private static SinkRecord Record(string json, long offset = 0) => new()
    {
        Topic = "orders",
        Partition = 0,
        Offset = offset,
        Value = Encoding.UTF8.GetBytes(json)
    };

    private sealed class RecordingProducer : IConnectProducer
    {
        public bool Fail { get; set; }

        public List<(string Topic, byte[]? Key, byte[] Value, IDictionary<string, byte[]>? Headers)> Produced { get; } = [];

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, CancellationToken cancellationToken = default)
            => ProduceAsync(topic, key, value, null, cancellationToken);

        public Task ProduceAsync(string topic, byte[]? key, byte[] value, IDictionary<string, byte[]>? headers,
            CancellationToken cancellationToken = default)
        {
            if (Fail)
            {
                throw new InvalidOperationException("broker unavailable");
            }

            Produced.Add((topic, key, value, headers));
            return Task.CompletedTask;
        }
    }
}
