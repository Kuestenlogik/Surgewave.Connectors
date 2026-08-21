using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Logic.Tests;

/// <summary>
/// Fan-in behaviour of the merge node.
/// </summary>
public class MergeTaskTests
{
    [Fact]
    public async Task PutAsync_FunnelsEveryInputTopicIntoTheOutputTopic()
    {
        var producer = new RecordingProducer();
        using var task = new MergeTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config(addSourceHeader: false));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("orders"), Record("shipments")], cts.Token);

        Assert.Equal(2, producer.Produced.Count);
        Assert.All(producer.Produced, p => Assert.Equal("merged", p.Topic));
    }

    [Fact]
    public async Task PutAsync_TagsRecordsWithTheirSourceTopic_WhenEnabled()
    {
        var producer = new RecordingProducer();
        using var task = new MergeTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config(addSourceHeader: true));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("orders")], cts.Token);

        var produced = Assert.Single(producer.Produced);
        Assert.Equal("orders", Encoding.UTF8.GetString(produced.Headers!["_source_topic"]));
    }

    [Fact]
    public async Task PutAsync_LeavesHeadersAloneWhenTaggingIsDisabled()
    {
        var producer = new RecordingProducer();
        using var task = new MergeTask();
        task.Initialize(new TaskContext { Producer = producer, RaiseError = _ => { } });
        task.Start(Config(addSourceHeader: false));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await task.PutAsync([Record("orders")], cts.Token);

        Assert.Null(Assert.Single(producer.Produced).Headers);
    }

    private static Dictionary<string, string> Config(bool addSourceHeader) => new()
    {
        [MergeConfig.OutputTopic] = "merged",
        [MergeConfig.AddSourceHeader] = addSourceHeader ? "true" : "false"
    };

    private static SinkRecord Record(string topic) => new()
    {
        Topic = topic,
        Partition = 0,
        Offset = 0,
        Value = Encoding.UTF8.GetBytes("""{"ok":true}""")
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
