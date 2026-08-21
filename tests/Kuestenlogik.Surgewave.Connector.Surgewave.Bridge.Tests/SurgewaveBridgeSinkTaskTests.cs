using System.Text;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Surgewave.Bridge.Tests;

/// <summary>
/// Tests for the routing decisions of <see cref="SurgewaveBridgeSinkTask"/>: which target topic
/// a mirrored record lands in, and which partition it gets when the source partitioning is not
/// preserved - pinning everything onto partition 0 would serialize the whole replication stream
/// through a single partition leader.
/// </summary>
public class SurgewaveBridgeSinkTaskTests
{
    private const string SourceTopic = "orders";

    [Fact]
    public void GetTargetTopic_MirrorsTheSourceTopicByDefault()
    {
        using var task = StartTask();

        Assert.Equal(SourceTopic, task.GetTargetTopic(SourceTopic));
    }

    [Fact]
    public void GetTargetTopic_AppliesTheTopicOverrideTemplate()
    {
        using var task = StartTask((SurgewaveBridgeConnectorConfig.Topic, "mirror-${topic}"));

        Assert.Equal("mirror-orders", task.GetTargetTopic(SourceTopic));
    }

    [Fact]
    public void GetTargetTopic_PrefixesWithTheTargetClusterAlias()
    {
        using var task = StartTask((SurgewaveBridgeConnectorConfig.TopicPrefixEnabled, "true"));

        Assert.Equal("west.orders", task.GetTargetTopic(SourceTopic));
    }

    [Fact]
    public void GetTargetTopic_DoesNotPrefixATopicThatAlreadyCarriesTheAlias()
    {
        using var task = StartTask(
            (SurgewaveBridgeConnectorConfig.TopicPrefixEnabled, "true"),
            (SurgewaveBridgeConnectorConfig.Topic, "west.orders"));

        Assert.Equal("west.orders", task.GetTargetTopic(SourceTopic));
    }

    [Fact]
    public void SelectPartition_WithASinglePartitionTopic_AlwaysPicksZero()
    {
        using var task = StartTask();

        Assert.Equal(0, task.SelectPartition(Record(key: "anything"), partitionCount: 1));
        Assert.Equal(0, task.SelectPartition(Record(key: null), partitionCount: 1));
    }

    [Fact]
    public void SelectPartition_SendsEveryRecordOfAKeyToTheSamePartition()
    {
        // Key ordering only survives replication if a key never changes partition.
        using var task = StartTask();

        var first = task.SelectPartition(Record("order-42"), partitionCount: 4);
        var second = task.SelectPartition(Record("order-42"), partitionCount: 4);

        Assert.Equal(first, second);
        Assert.InRange(first, 0, 3);
    }

    [Fact]
    public void SelectPartition_SpreadsUnkeyedRecordsOverAllPartitions()
    {
        // The whole point of turning partition preservation off: unkeyed traffic has to fan
        // out instead of piling onto partition 0.
        using var task = StartTask();

        var picks = new List<int>();
        for (var i = 0; i < 4; i++)
        {
            picks.Add(task.SelectPartition(Record(key: null), partitionCount: 3));
        }

        Assert.Equal(new[] { 0, 1, 2, 0 }, picks);
    }

    private static SurgewaveBridgeSinkTask StartTask(params (string Key, string Value)[] settings)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SurgewaveBridgeConnectorConfig.TargetBootstrapServers] = "localhost:9092",
            [SurgewaveBridgeConnectorConfig.TargetClusterAlias] = "west",
            [SurgewaveBridgeConnectorConfig.PreservePartitions] = "false"
        };

        foreach (var (key, value) in settings)
        {
            config[key] = value;
        }

        var task = new SurgewaveBridgeSinkTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.Start(config);
        return task;
    }

    private static SinkRecord Record(string? key) => new()
    {
        Topic = SourceTopic,
        Partition = 0,
        Offset = 17,
        Key = key == null ? null : Encoding.UTF8.GetBytes(key),
        Value = Encoding.UTF8.GetBytes("payload")
    };
}
