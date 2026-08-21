using Kuestenlogik.Surgewave.Connect;
using StackExchange.Redis;

namespace Kuestenlogik.Surgewave.Connector.Redis.List.Tests;

/// <summary>
/// The source reads through an LMOVE into a processing list so a crash between reading and
/// producing cannot lose items. These tests pin the two names/directions that contract rests on.
/// </summary>
public class RedisListSourceTaskTests
{
    [Theory]
    [InlineData("left", ListSide.Left)]
    [InlineData("LEFT", ListSide.Left)]
    [InlineData("right", ListSide.Right)]
    [InlineData("Right", ListSide.Right)]
    [InlineData("", ListSide.Left)]
    [InlineData("sideways", ListSide.Left)]
    public void ConsumeSide_MapsThePopDirection(string popDirection, ListSide expected)
    {
        Assert.Equal(expected, RedisListSourceTask.ConsumeSide(popDirection));
    }

    [Fact]
    public void ProcessingKeyFor_NamesTheInFlightList()
    {
        // Items live here between the read and the commit; recovery after a crash looks for
        // exactly this key, so the naming is part of the connector's contract.
        Assert.Equal("orders:processing", RedisListSourceTask.ProcessingKeyFor("orders"));
    }

    [Fact]
    public void Start_WithoutTopic_FailsBeforeConnecting()
    {
        using var task = new RedisListSourceTask();
        task.Initialize(new TaskContext());

        var config = new Dictionary<string, string>
        {
            [RedisListConnectorConfig.Key] = "orders"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public void Start_WithNonNumericBatchSize_FailsBeforeConnecting()
    {
        using var task = new RedisListSourceTask();
        task.Initialize(new TaskContext());

        var config = new Dictionary<string, string>
        {
            [RedisListConnectorConfig.Key] = "orders",
            [RedisListConnectorConfig.Topic] = "orders-topic",
            [RedisListConnectorConfig.BatchSize] = "many"
        };

        Assert.Throws<FormatException>(() => task.Start(config));
    }

    [Fact]
    public async Task CommitAsync_WithoutAConnection_DoesNothing()
    {
        // Commits arrive on the worker's schedule and may reach a task that never connected;
        // remembering the value for the next attempt is fine, throwing is not.
        using var task = new RedisListSourceTask();
        task.Initialize(new TaskContext { RaiseError = _ => { } });

        task.CommitRecord(
            new SourceRecord
            {
                SourcePartition = new Dictionary<string, object> { ["key"] = "orders" },
                SourceOffset = new Dictionary<string, object> { ["offset"] = 1L },
                Topic = "orders-topic",
                Value = "payload"u8.ToArray()
            },
            new RecordMetadata { Topic = "orders-topic", Partition = 0, Offset = 5 });

        await task.CommitAsync(CancellationToken.None);
    }
}
