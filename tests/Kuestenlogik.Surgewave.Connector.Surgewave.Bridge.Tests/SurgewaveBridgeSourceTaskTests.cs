using System.Globalization;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Surgewave.Bridge.Tests;

/// <summary>
/// Tests for the replication cursor of <see cref="SurgewaveBridgeSourceTask"/>. Replication is
/// only resumable if the task reads back what the previous run committed and republishes it as
/// a checkpoint - a task that always restarts at offset 0 (or at the latest offset) either
/// duplicates or silently skips every message the failed run had already handled.
/// </summary>
public class SurgewaveBridgeSourceTaskTests
{
    private const string SourceTopic = "orders";

    [Fact]
    public async Task ResolveStartOffsetAsync_ResumesAfterTheLastReplicatedOffset()
    {
        var reader = new FakeOffsetStorageReader();
        reader.Store(SourceTopic, partition: 0, offset: 41);
        using var task = StartTask(reader);

        var offset = await task.ResolveStartOffsetAsync(SourceTopic, 0, CancellationToken.None);

        // The stored value is the offset of the last replicated message, so replication
        // continues with the one after it.
        Assert.Equal(42L, offset);
    }

    [Fact]
    public async Task ResolveStartOffsetAsync_WithNothingReplicatedYet_StartsAtTheBeginning()
    {
        var reader = new FakeOffsetStorageReader();
        using var task = StartTask(reader);

        var offset = await task.ResolveStartOffsetAsync(SourceTopic, 3, CancellationToken.None);

        Assert.Equal(0L, offset);
        Assert.Equal("orders/3", Assert.Single(reader.Requested));
    }

    [Fact]
    public async Task ResolveStartOffsetAsync_AsksForTheCursorOfEachPartitionSeparately()
    {
        var reader = new FakeOffsetStorageReader();
        reader.Store(SourceTopic, partition: 0, offset: 7);
        reader.Store(SourceTopic, partition: 1, offset: 90);
        using var task = StartTask(reader);

        var first = await task.ResolveStartOffsetAsync(SourceTopic, 0, CancellationToken.None);
        var second = await task.ResolveStartOffsetAsync(SourceTopic, 1, CancellationToken.None);

        Assert.Equal(8L, first);
        Assert.Equal(91L, second);
    }

    [Fact]
    public async Task ResolveStartOffsetAsync_WithOffsetTrackingDisabled_NeverAsksTheOffsetStore()
    {
        var reader = new FakeOffsetStorageReader();
        reader.Store(SourceTopic, partition: 0, offset: 41);
        using var task = StartTask(reader, (SurgewaveBridgeConnectorConfig.OffsetTrackingEnabled, "false"));

        var offset = await task.ResolveStartOffsetAsync(SourceTopic, 0, CancellationToken.None);

        Assert.Equal(0L, offset);
        Assert.Empty(reader.Requested);
    }

    [Fact]
    public void CreateCheckpointRecord_BeforeAnythingIsCommitted_ReturnsNothing()
    {
        using var task = StartTask();

        Assert.Null(task.CreateCheckpointRecord());
    }

    [Fact]
    public void CommitRecord_PutsTheConfirmedOffsetIntoTheCheckpoint()
    {
        using var task = StartTask();

        task.CommitRecord(DataRecord(SourceTopic, partition: 2, offset: 41), Metadata());

        var checkpoint = task.CreateCheckpointRecord();
        Assert.NotNull(checkpoint);
        Assert.Equal("east.checkpoints", checkpoint.Topic);

        var entry = SingleCheckpointEntry(checkpoint);
        Assert.Equal(SourceTopic, entry.GetProperty("topic").GetString());
        Assert.Equal(2, entry.GetProperty("partition").GetInt32());
        Assert.Equal(41L, entry.GetProperty("offset").GetInt64());
    }

    [Fact]
    public void CommitRecord_KeepsTheHighestOffsetPerPartition()
    {
        // Commits can arrive out of order; the checkpoint must never walk backwards or the
        // failover target would replay messages that were already delivered.
        using var task = StartTask();

        task.CommitRecord(DataRecord(SourceTopic, partition: 0, offset: 41), Metadata());
        task.CommitRecord(DataRecord(SourceTopic, partition: 0, offset: 12), Metadata());

        var entry = SingleCheckpointEntry(task.CreateCheckpointRecord());
        Assert.Equal(41L, entry.GetProperty("offset").GetInt64());
    }

    [Fact]
    public void CommitRecord_IgnoresRecordsThatCarryNoReplicationCursor()
    {
        // Heartbeats and checkpoints ride the same stream but describe no source partition.
        using var task = StartTask();

        task.CommitRecord(
            new SourceRecord
            {
                SourcePartition = new Dictionary<string, object> { ["type"] = "heartbeat" },
                SourceOffset = new Dictionary<string, object> { ["id"] = 1L },
                Topic = "east.heartbeats",
                Value = [1, 2, 3]
            },
            Metadata());

        Assert.Null(task.CreateCheckpointRecord());
    }

    [Fact]
    public void GetTargetTopic_PrefixesTheSourceTopicWithTheClusterAlias()
    {
        using var task = StartTask();

        Assert.Equal("east.orders", task.GetTargetTopic(SourceTopic));
    }

    [Fact]
    public void GetTargetTopic_WithoutPrefixing_KeepsThePlainSourceTopic()
    {
        using var task = StartTask(reader: null, (SurgewaveBridgeConnectorConfig.TopicPrefixEnabled, "false"));

        Assert.Equal("orders", task.GetTargetTopic(SourceTopic));
    }

    private static SurgewaveBridgeSourceTask StartTask(
        IOffsetStorageReader? reader = null,
        params (string Key, string Value)[] settings)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SurgewaveBridgeConnectorConfig.SourceBootstrapServers] = "localhost:9092",
            [SurgewaveBridgeConnectorConfig.SourceClusterAlias] = "east",
            [SurgewaveBridgeConnectorConfig.Topics] = SourceTopic,
            [SurgewaveBridgeConnectorConfig.Topic] = "${source.topic}"
        };

        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new SurgewaveBridgeSourceTask();
        task.Initialize(new TaskContext { OffsetStorageReader = reader, RaiseError = _ => { } });
        task.Start(config);
        return task;
    }

    private static JsonElement SingleCheckpointEntry(SourceRecord? checkpoint)
    {
        Assert.NotNull(checkpoint);
        using var document = JsonDocument.Parse(checkpoint.Value);
        var offsets = document.RootElement.GetProperty("offsets");
        Assert.Equal(1, offsets.GetArrayLength());
        return offsets[0].Clone();
    }

    private static SourceRecord DataRecord(string topic, int partition, long offset) => new()
    {
        SourcePartition = new Dictionary<string, object>
        {
            ["cluster"] = "east",
            ["topic"] = topic,
            ["partition"] = partition
        },
        SourceOffset = new Dictionary<string, object> { ["offset"] = offset },
        Topic = "east." + topic,
        Value = [1, 2, 3]
    };

    private static RecordMetadata Metadata() => new()
    {
        Topic = "east.orders",
        Partition = 0,
        Offset = 5
    };

    private sealed class FakeOffsetStorageReader : IOffsetStorageReader
    {
        private readonly Dictionary<string, IDictionary<string, object>> _offsets = new(StringComparer.Ordinal);

        public List<string> Requested { get; } = [];

        public void Store(string topic, int partition, long offset) =>
            _offsets[Key(topic, partition)] = new Dictionary<string, object> { ["offset"] = offset };

        public IDictionary<string, object>? Offset(IDictionary<string, object> partition)
        {
            var key = Key(
                (string)partition["topic"],
                Convert.ToInt32(partition["partition"], CultureInfo.InvariantCulture));

            Requested.Add(key);
            return _offsets.GetValueOrDefault(key);
        }

        public IDictionary<IDictionary<string, object>, IDictionary<string, object>> Offsets(
            IReadOnlyCollection<IDictionary<string, object>> partitions) => throw new NotSupportedException();

        private static string Key(string topic, int partition) =>
            topic + "/" + partition.ToString(CultureInfo.InvariantCulture);
    }
}
