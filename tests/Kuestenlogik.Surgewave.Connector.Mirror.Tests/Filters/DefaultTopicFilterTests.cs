using Kuestenlogik.Surgewave.Connector.Mirror.Filters;
using Kuestenlogik.Surgewave.Connector.Mirror.Policies;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests.Filters;

public class DefaultTopicFilterTests
{
    [Fact]
    public void ShouldReplicate_WithMatchAllPattern_ShouldMatchAllNonInternal()
    {
        var filter = new DefaultTopicFilter(".*", [], [], new DefaultReplicationPolicy());

        Assert.True(filter.ShouldReplicate("orders"));
        Assert.True(filter.ShouldReplicate("payments"));
        Assert.True(filter.ShouldReplicate("users.events"));
    }

    [Fact]
    public void ShouldReplicate_ShouldExcludeInternalTopics()
    {
        var filter = new DefaultTopicFilter(".*", [], [], new DefaultReplicationPolicy());

        Assert.False(filter.ShouldReplicate("__consumer_offsets"));
        Assert.False(filter.ShouldReplicate("dc1.heartbeats"));
        Assert.False(filter.ShouldReplicate("dc1.checkpoints.internal"));
    }

    [Fact]
    public void ShouldReplicate_WithSpecificPattern_ShouldOnlyMatchPattern()
    {
        var filter = new DefaultTopicFilter("orders.*", [], [], new DefaultReplicationPolicy());

        Assert.True(filter.ShouldReplicate("orders"));
        Assert.True(filter.ShouldReplicate("orders.created"));
        Assert.True(filter.ShouldReplicate("orders.updated"));
        Assert.False(filter.ShouldReplicate("payments"));
    }

    [Fact]
    public void ShouldReplicate_WithWhitelist_ShouldOnlyMatchWhitelist()
    {
        var filter = new DefaultTopicFilter(".*", ["orders", "payments"], [], new DefaultReplicationPolicy());

        Assert.True(filter.ShouldReplicate("orders"));
        Assert.True(filter.ShouldReplicate("payments"));
        Assert.False(filter.ShouldReplicate("users"));
    }

    [Fact]
    public void ShouldReplicate_WithBlacklist_ShouldExcludeBlacklisted()
    {
        var filter = new DefaultTopicFilter(".*", [], ["internal", "test"], new DefaultReplicationPolicy());

        Assert.True(filter.ShouldReplicate("orders"));
        Assert.True(filter.ShouldReplicate("payments"));
        Assert.False(filter.ShouldReplicate("internal"));
        Assert.False(filter.ShouldReplicate("test"));
    }

    [Fact]
    public void ShouldReplicate_BlacklistTakesPrecedenceOverWhitelist()
    {
        var filter = new DefaultTopicFilter(".*", ["orders", "internal"], ["internal"], new DefaultReplicationPolicy());

        Assert.True(filter.ShouldReplicate("orders"));
        Assert.False(filter.ShouldReplicate("internal"));
    }

    [Fact]
    public void FilterTopics_ShouldReturnOnlyMatchingTopics()
    {
        var filter = new DefaultTopicFilter("orders.*", [], [], new DefaultReplicationPolicy());
        var topics = new[] { "orders", "orders.created", "payments", "users" };

        var result = filter.FilterTopics(topics).ToList();

        Assert.Equal(2, result.Count);
        Assert.Contains("orders", result);
        Assert.Contains("orders.created", result);
    }

    [Fact]
    public void FilterTopics_ShouldExcludeBlacklistedTopics()
    {
        var filter = new DefaultTopicFilter(".*", [], ["test", "internal"], new DefaultReplicationPolicy());
        var topics = new[] { "orders", "test", "payments", "internal" };

        var result = filter.FilterTopics(topics).ToList();

        Assert.Equal(2, result.Count);
        Assert.Contains("orders", result);
        Assert.Contains("payments", result);
    }

    [Fact]
    public void FromConfig_ShouldCreateFilterFromConfiguration()
    {
        var config = new MirrorMakerConfig
        {
            SourceClusterAlias = "dc1",
            TargetClusterAlias = "dc2",
            SourceBootstrapServers = "localhost:9092",
            TargetBootstrapServers = "remote:9092",
            TopicsPattern = "orders.*",
            TopicsWhitelist = [],
            TopicsBlacklist = ["test"]
        };

        var filter = DefaultTopicFilter.FromConfig(config, new DefaultReplicationPolicy());

        Assert.True(filter.ShouldReplicate("orders"));
        Assert.True(filter.ShouldReplicate("orders.created"));
        Assert.False(filter.ShouldReplicate("payments"));
        Assert.False(filter.ShouldReplicate("test"));
    }
}
