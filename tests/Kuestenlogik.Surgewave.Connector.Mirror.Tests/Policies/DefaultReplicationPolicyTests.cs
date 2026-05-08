using Kuestenlogik.Surgewave.Connector.Mirror.Policies;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests.Policies;

public class DefaultReplicationPolicyTests
{
    private readonly DefaultReplicationPolicy _policy = new();

    [Theory]
    [InlineData("orders", "dc1", "dc1.orders")]
    [InlineData("payments", "us-east", "us-east.payments")]
    // Note: Topics with dots (e.g., "users.events") are treated as already prefixed
    // to prevent double-prefixing. Use underscores for namespacing in original topics.
    [InlineData("users_events", "cluster-a", "cluster-a.users_events")]
    public void FormatRemoteTopic_ShouldPrefixWithSourceCluster(string topic, string sourceCluster, string expected)
    {
        var result = _policy.FormatRemoteTopic(sourceCluster, topic);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void FormatRemoteTopic_ShouldNotDoublePrefixAlreadyRemoteTopic()
    {
        var alreadyRemote = "dc1.orders";
        var result = _policy.FormatRemoteTopic("dc2", alreadyRemote);
        Assert.Equal("dc1.orders", result);
    }

    [Theory]
    [InlineData("dc1.orders", "orders")]
    [InlineData("us-east.payments", "payments")]
    [InlineData("cluster-a.users.events", "users.events")]
    public void OriginalTopic_ShouldRemoveClusterPrefix(string topic, string expected)
    {
        var result = _policy.OriginalTopic(topic);
        Assert.Equal(expected, result);
    }

    [Fact]
    public void OriginalTopic_ShouldReturnSameTopicIfNoPrefix()
    {
        var result = _policy.OriginalTopic("orders");
        Assert.Equal("orders", result);
    }

    [Theory]
    [InlineData("dc1.orders", "dc1")]
    [InlineData("us-east.payments", "us-east")]
    [InlineData("cluster-a.users.events", "cluster-a")]
    public void TopicSource_ShouldExtractClusterAlias(string topic, string expectedSource)
    {
        var result = _policy.TopicSource(topic);
        Assert.Equal(expectedSource, result);
    }

    [Fact]
    public void TopicSource_ShouldReturnNullForLocalTopic()
    {
        var result = _policy.TopicSource("orders");
        Assert.Null(result);
    }

    [Theory]
    [InlineData("dc1.orders", "dc1", true)]
    [InlineData("dc1.orders", "dc2", false)]
    [InlineData("orders", "dc1", false)]
    public void IsFromCluster_ShouldDetectClusterOrigin(string topic, string cluster, bool expected)
    {
        var result = _policy.IsFromCluster(topic, cluster);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("dc1.heartbeats", true)]
    [InlineData("dc1.checkpoints.internal", true)]
    [InlineData("mm2-offset-syncs.dc1.offset-syncs.internal", true)]
    [InlineData("__consumer_offsets", true)]
    [InlineData("orders", false)]
    [InlineData("dc1.orders", false)]
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
        Assert.Equal("dc1->dc2.checkpoints.internal", result);
    }

    [Fact]
    public void OffsetSyncTopic_ShouldFormatCorrectly()
    {
        var result = _policy.OffsetSyncTopic("dc1");
        Assert.Equal("mm2-offset-syncs.dc1.offset-syncs.internal", result);
    }

    [Fact]
    public void WouldCreateLoop_ShouldDetectLoopFromTargetCluster()
    {
        // Topic originated from dc2, trying to replicate back to dc2 would create a loop
        var result = _policy.WouldCreateLoop("dc2.orders", "dc1", "dc2");
        Assert.True(result);
    }

    [Fact]
    public void WouldCreateLoop_ShouldAllowNormalReplication()
    {
        // Topic originated from dc1, replicating to dc2 is fine
        var result = _policy.WouldCreateLoop("dc1.orders", "dc1", "dc2");
        Assert.False(result);
    }

    [Fact]
    public void WouldCreateLoop_ShouldAllowLocalTopicReplication()
    {
        // Local topic can be replicated
        var result = _policy.WouldCreateLoop("orders", "dc1", "dc2");
        Assert.False(result);
    }

    [Fact]
    public void GetUpstreamPath_ShouldReturnEmptyForLocalTopic()
    {
        var path = _policy.GetUpstreamPath("orders");
        Assert.Empty(path);
    }

    [Fact]
    public void GetUpstreamPath_ShouldReturnSingleClusterForOnceReplicated()
    {
        var path = _policy.GetUpstreamPath("dc1.orders");
        Assert.Single(path);
        Assert.Equal("dc1", path[0]);
    }

    [Theory]
    [InlineData("-")]
    [InlineData("_")]
    [InlineData("::")]
    public void CustomSeparator_ShouldWorkCorrectly(string separator)
    {
        var policy = new DefaultReplicationPolicy(separator);
        var topic = policy.FormatRemoteTopic("dc1", "orders");
        Assert.Equal($"dc1{separator}orders", topic);
    }
}
