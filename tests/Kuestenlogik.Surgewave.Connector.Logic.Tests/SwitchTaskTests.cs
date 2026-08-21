using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Logic.Tests;

/// <summary>
/// Routing rules of the switch node: a record goes to the topic mapped to its field value, to the
/// default topic, or nowhere at all.
/// </summary>
public class SwitchTaskTests
{
    private const string Cases = "order:orders-topic,alert:alerts-topic";

    [Fact]
    public async Task PutAsync_RoutesToTheMappedTopic()
    {
        var producer = new RecordingProducer();
        using var task = new SwitchTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("type", Cases));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"type":"alert"}""")], cts.Token);

        Assert.Equal("alerts-topic", Assert.Single(producer.Produced).Topic);
    }

    [Fact]
    public async Task PutAsync_MatchesCaseValuesCaseInsensitively()
    {
        var producer = new RecordingProducer();
        using var task = new SwitchTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("type", Cases));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"type":"ALERT"}""")], cts.Token);

        Assert.Equal("alerts-topic", Assert.Single(producer.Produced).Topic);
    }

    [Fact]
    public async Task PutAsync_ResolvesNestedFieldPaths()
    {
        var producer = new RecordingProducer();
        using var task = new SwitchTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("event.category", Cases));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"event":{"category":"order"}}""")], cts.Token);

        Assert.Equal("orders-topic", Assert.Single(producer.Produced).Topic);
    }

    [Fact]
    public async Task PutAsync_FallsBackToTheDefaultTopic()
    {
        var producer = new RecordingProducer();
        using var task = new SwitchTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("type", Cases, defaultTopic: "other-topic"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"type":"unknown"}""")], cts.Token);

        Assert.Equal("other-topic", Assert.Single(producer.Produced).Topic);
    }

    [Fact]
    public async Task PutAsync_DropsUnmatchedRecords_WhenNoDefaultTopicIsConfigured()
    {
        var producer = new RecordingProducer();
        using var task = new SwitchTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("type", Cases));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("""{"type":"unknown"}""")], cts.Token);

        Assert.Empty(producer.Produced);
    }

    [Fact]
    public async Task PutAsync_ForwardsKeyValueAndHeadersUnchanged()
    {
        var producer = new RecordingProducer();
        using var task = new SwitchTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config("type", Cases));

        var record = new SinkRecord
        {
            Topic = "input",
            Partition = 0,
            Offset = 0,
            Key = Encoding.UTF8.GetBytes("k-1"),
            Value = Encoding.UTF8.GetBytes("""{"type":"order"}"""),
            Headers = new Dictionary<string, byte[]>
            {
                ["trace-id"] = Encoding.UTF8.GetBytes("abc")
            }
        };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([record], cts.Token);

        var produced = Assert.Single(producer.Produced);
        Assert.Equal("k-1", Encoding.UTF8.GetString(produced.Key!));
        Assert.Equal("""{"type":"order"}""", Encoding.UTF8.GetString(produced.Value));
        Assert.Equal("abc", Encoding.UTF8.GetString(produced.Headers!["trace-id"]));
    }

    private static Dictionary<string, string> Config(string fieldPath, string cases, string defaultTopic = "") => new()
    {
        [SwitchConfig.FieldPath] = fieldPath,
        [SwitchConfig.Cases] = cases,
        [SwitchConfig.DefaultTopic] = defaultTopic
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
