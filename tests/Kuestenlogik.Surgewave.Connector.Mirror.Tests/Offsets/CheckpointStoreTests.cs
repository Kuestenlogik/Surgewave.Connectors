using Kuestenlogik.Surgewave.Connector.Mirror.Offsets;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests.Offsets;

public class CheckpointStoreTests
{
    [Fact]
    public void Store_ShouldStoreCheckpoint()
    {
        var store = new CheckpointStore();
        var checkpoint = new Checkpoint
        {
            ConsumerGroup = "my-app",
            Topic = "orders",
            Partition = 0,
            SourceOffset = 100,
            TargetOffset = 95,
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        store.Store(checkpoint);

        var result = store.Get("my-app", "orders", 0);
        Assert.NotNull(result);
        Assert.Equal(100, result.Value.SourceOffset);
        Assert.Equal(95, result.Value.TargetOffset);
    }

    [Fact]
    public void Get_ShouldReturnNullForNonexistent()
    {
        var store = new CheckpointStore();

        var result = store.Get("my-app", "orders", 0);
        Assert.Null(result);
    }

    [Fact]
    public void Store_ShouldUpdateExistingCheckpoint()
    {
        var store = new CheckpointStore();

        store.Store(new Checkpoint
        {
            ConsumerGroup = "my-app",
            Topic = "orders",
            Partition = 0,
            SourceOffset = 100,
            TargetOffset = 95,
            Timestamp = 1000
        });

        store.Store(new Checkpoint
        {
            ConsumerGroup = "my-app",
            Topic = "orders",
            Partition = 0,
            SourceOffset = 200,
            TargetOffset = 195,
            Timestamp = 2000
        });

        var result = store.Get("my-app", "orders", 0);
        Assert.NotNull(result);
        Assert.Equal(200, result.Value.SourceOffset);
        Assert.Equal(195, result.Value.TargetOffset);
    }

    [Fact]
    public void GetForGroup_ShouldReturnAllCheckpointsForGroup()
    {
        var store = new CheckpointStore();

        store.Store(new Checkpoint
        {
            ConsumerGroup = "my-app",
            Topic = "orders",
            Partition = 0,
            SourceOffset = 100,
            TargetOffset = 95,
            Timestamp = 1000
        });

        store.Store(new Checkpoint
        {
            ConsumerGroup = "my-app",
            Topic = "orders",
            Partition = 1,
            SourceOffset = 200,
            TargetOffset = 195,
            Timestamp = 1000
        });

        store.Store(new Checkpoint
        {
            ConsumerGroup = "other-app",
            Topic = "orders",
            Partition = 0,
            SourceOffset = 300,
            TargetOffset = 295,
            Timestamp = 1000
        });

        var result = store.GetForGroup("my-app");

        Assert.Equal(2, result.Count);
        Assert.All(result, c => Assert.Equal("my-app", c.ConsumerGroup));
    }

    [Fact]
    public void All_ShouldReturnAllCheckpoints()
    {
        var store = new CheckpointStore();

        store.Store(new Checkpoint
        {
            ConsumerGroup = "my-app",
            Topic = "orders",
            Partition = 0,
            SourceOffset = 100,
            TargetOffset = 95,
            Timestamp = 1000
        });

        store.Store(new Checkpoint
        {
            ConsumerGroup = "other-app",
            Topic = "payments",
            Partition = 0,
            SourceOffset = 200,
            TargetOffset = 195,
            Timestamp = 1000
        });

        var all = store.All;
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public void Clear_ShouldRemoveAllCheckpoints()
    {
        var store = new CheckpointStore();

        store.Store(new Checkpoint
        {
            ConsumerGroup = "my-app",
            Topic = "orders",
            Partition = 0,
            SourceOffset = 100,
            TargetOffset = 95,
            Timestamp = 1000
        });

        store.Store(new Checkpoint
        {
            ConsumerGroup = "other-app",
            Topic = "payments",
            Partition = 0,
            SourceOffset = 200,
            TargetOffset = 195,
            Timestamp = 1000
        });

        store.Clear();

        Assert.Empty(store.All);
        Assert.Null(store.Get("my-app", "orders", 0));
    }

    [Fact]
    public void ShouldHandleMultipleGroupsAndTopics()
    {
        var store = new CheckpointStore();
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Store checkpoints for multiple groups and topics
        store.Store(new Checkpoint { ConsumerGroup = "app-1", Topic = "orders", Partition = 0, SourceOffset = 100, TargetOffset = 95, Timestamp = timestamp });
        store.Store(new Checkpoint { ConsumerGroup = "app-1", Topic = "payments", Partition = 0, SourceOffset = 200, TargetOffset = 195, Timestamp = timestamp });
        store.Store(new Checkpoint { ConsumerGroup = "app-2", Topic = "orders", Partition = 0, SourceOffset = 300, TargetOffset = 295, Timestamp = timestamp });

        Assert.Equal(3, store.All.Count);
        Assert.Equal(2, store.GetForGroup("app-1").Count);
        Assert.Single(store.GetForGroup("app-2"));
    }
}
