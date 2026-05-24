using Kuestenlogik.Surgewave.Connector.Mirror.Policies;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests.Policies;

public class IdentityReplicationPolicyTests
{
    private readonly IdentityReplicationPolicy _policy = new();

    [Theory]
    [InlineData("orders", "dc1", "orders")]
    [InlineData("payments", "us-east", "payments")]
    [InlineData("users.events", "cluster-a", "users.events")]
    public void FormatRemoteTopic_ShouldKeepTopicUnchanged(string topic, string sourceCluster, string expected)
    {
        var result = _policy.FormatRemoteTopic(sourceCluster, topic);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void OriginalTopic_ShouldReturnSameTopic()
    {
        var result = _policy.OriginalTopic("orders");
        Assert.Equal("orders", result);
    }

    [Fact]
    public void TopicSource_ShouldAlwaysReturnNull()
    {
        var result = _policy.TopicSource("dc1.orders");
        Assert.Null(result);
    }

    [Theory]
    [InlineData("dc1.orders", "dc1", false)]
    [InlineData("orders", "dc1", false)]
    public void IsFromCluster_ShouldAlwaysReturnFalse(string topic, string cluster, bool expected)
    {
        var result = _policy.IsFromCluster(topic, cluster);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("mm2.heartbeats", true)]
    [InlineData("mm2.checkpoints.internal", true)]
    [InlineData("mm2.offset-syncs.internal", true)]
    [InlineData("dc1.heartbeats", true)]
    [InlineData("dc1.checkpoints.internal", true)]
    [InlineData("__consumer_offsets", true)]
    [InlineData("orders", false)]
    public void IsInternalTopic_ShouldIdentifyInternalTopics(string topic, bool expected)
    {
        var result = _policy.IsInternalTopic(topic);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void HeartbeatTopic_ShouldFormatCorrectly()
    {
        var result = _policy.HeartbeatTopic("dc1");
        Assert.Equal("dc1.heartbeats", result);
    }

    [Fact]
    public void CheckpointTopic_ShouldFormatCorrectly()
    {
        var result = _policy.CheckpointTopic("dc1", "dc2");
        Assert.Equal("dc1.checkpoints.internal", result);
    }

    [Fact]
    public void OffsetSyncTopic_ShouldFormatCorrectly()
    {
        var result = _policy.OffsetSyncTopic("dc1");
        Assert.Equal("dc1.offset-syncs.internal", result);
    }
}
