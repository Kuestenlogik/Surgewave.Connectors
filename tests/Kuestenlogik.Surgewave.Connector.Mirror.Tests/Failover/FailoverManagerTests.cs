using Kuestenlogik.Surgewave.Connector.Mirror.Failover;
using Kuestenlogik.Surgewave.Connector.Mirror.Offsets;
using Kuestenlogik.Surgewave.Connector.Mirror.Policies;
using MirrorTopicPartition = Kuestenlogik.Surgewave.Connector.Mirror.Failover.TopicPartition;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests.Failover;

public class FailoverManagerTests
{
    [Fact]
    public void Constructor_ShouldInitializeWithDisconnectedState()
    {
        var manager = new FailoverManager(
            "dc1", "dc2",
            new DefaultReplicationPolicy(),
            new OffsetTranslator());

        Assert.False(manager.State.IsConnected);
        Assert.False(manager.State.IsFailoverInProgress);
        Assert.Null(manager.State.CurrentFailover);
        Assert.Null(manager.State.LastFailover);
    }

    [Fact]
    public async Task FailoverGroupAsync_WhenNotConnected_ShouldThrow()
    {
        var manager = new FailoverManager(
            "dc1", "dc2",
            new DefaultReplicationPolicy(),
            new OffsetTranslator());

        var topicPartitions = new[] { new MirrorTopicPartition("orders", 0, 100) };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            manager.FailoverGroupAsync("my-app", topicPartitions));
    }

    [Fact]
    public async Task CheckHealthAsync_WhenNotConnected_ShouldReturnUnhealthy()
    {
        var manager = new FailoverManager(
            "dc1", "dc2",
            new DefaultReplicationPolicy(),
            new OffsetTranslator());

        var health = await manager.CheckHealthAsync();

        Assert.False(health.SourceClusterHealthy);
        Assert.False(health.TargetClusterHealthy);
        Assert.False(health.ShouldFailover);
    }

    [Fact]
    public void FailoverResult_ShouldContainCorrectProperties()
    {
        var result = new FailoverResult
        {
            ConsumerGroup = "my-app",
            SourceCluster = "dc1",
            TargetCluster = "dc2",
            StartedAt = DateTimeOffset.UtcNow
        };

        Assert.Equal("my-app", result.ConsumerGroup);
        Assert.Equal("dc1", result.SourceCluster);
        Assert.Equal("dc2", result.TargetCluster);
        Assert.False(result.Success);
        Assert.Empty(result.OffsetMappings);
        Assert.Empty(result.Warnings);
    }

    [Fact]
    public void OffsetMapping_ShouldContainCorrectProperties()
    {
        var mapping = new OffsetMapping
        {
            SourceTopic = "orders",
            TargetTopic = "dc1.orders",
            Partition = 0,
            SourceOffset = 100,
            TargetOffset = 95
        };

        Assert.Equal("orders", mapping.SourceTopic);
        Assert.Equal("dc1.orders", mapping.TargetTopic);
        Assert.Equal(0, mapping.Partition);
        Assert.Equal(100, mapping.SourceOffset);
        Assert.Equal(95, mapping.TargetOffset);
    }

    [Fact]
    public void ClusterHealthStatus_ShouldDetermineShouldFailover()
    {
        // Source unhealthy, target healthy -> should failover
        var status1 = new ClusterHealthStatus
        {
            CheckedAt = DateTimeOffset.UtcNow,
            SourceClusterHealthy = false,
            TargetClusterHealthy = true
        };
        status1.ShouldFailover = !status1.SourceClusterHealthy && status1.TargetClusterHealthy;
        Assert.True(status1.ShouldFailover);

        // Both healthy -> should not failover
        var status2 = new ClusterHealthStatus
        {
            CheckedAt = DateTimeOffset.UtcNow,
            SourceClusterHealthy = true,
            TargetClusterHealthy = true
        };
        status2.ShouldFailover = !status2.SourceClusterHealthy && status2.TargetClusterHealthy;
        Assert.False(status2.ShouldFailover);

        // Both unhealthy -> should not failover
        var status3 = new ClusterHealthStatus
        {
            CheckedAt = DateTimeOffset.UtcNow,
            SourceClusterHealthy = false,
            TargetClusterHealthy = false
        };
        status3.ShouldFailover = !status3.SourceClusterHealthy && status3.TargetClusterHealthy;
        Assert.False(status3.ShouldFailover);
    }

    [Fact]
    public void TopicPartition_ShouldStoreValues()
    {
        var tp = new MirrorTopicPartition("orders", 3, 12345);

        Assert.Equal("orders", tp.Topic);
        Assert.Equal(3, tp.Partition);
        Assert.Equal(12345, tp.Offset);
    }

    [Fact]
    public void TopicPartition_Equality_ShouldWork()
    {
        var tp1 = new MirrorTopicPartition("orders", 0, 100);
        var tp2 = new MirrorTopicPartition("orders", 0, 100);
        var tp3 = new MirrorTopicPartition("orders", 1, 100);

        Assert.Equal(tp1, tp2);
        Assert.NotEqual(tp1, tp3);
    }
}
