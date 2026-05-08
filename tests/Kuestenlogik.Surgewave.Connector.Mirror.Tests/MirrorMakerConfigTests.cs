namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests;

public class MirrorMakerConfigTests
{
    private static readonly string[] OrdersPayments = ["orders", "payments"];
    private static readonly string[] TestInternal = ["test", "internal"];
    private static readonly string[] App1App2 = ["app-1", "app-2"];
    private static readonly string[] OrdersPaymentsUsers = ["orders", "payments", "users"];

    [Fact]
    public void FromDictionary_ShouldParseRequiredFields()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.cluster.alias"] = "dc1",
            ["target.cluster.alias"] = "dc2",
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal("dc1", config.SourceClusterAlias);
        Assert.Equal("dc2", config.TargetClusterAlias);
        Assert.Equal("localhost:9092", config.SourceBootstrapServers);
        Assert.Equal("remote:9092", config.TargetBootstrapServers);
    }

    [Fact]
    public void FromDictionary_ShouldUseDefaults()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal("source", config.SourceClusterAlias);
        Assert.Equal("target", config.TargetClusterAlias);
        Assert.Equal(".*", config.TopicsPattern);
        Assert.Empty(config.TopicsWhitelist);
        Assert.Empty(config.TopicsBlacklist);
        Assert.Equal(1, config.TasksMax);
        Assert.True(config.SyncTopicConfigs);
        Assert.True(config.EmitHeartbeats);
        Assert.True(config.EmitCheckpoints);
    }

    [Fact]
    public void FromDictionary_ShouldParseTopicFiltering()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["topics"] = "orders.*",
            ["topics.whitelist"] = "orders,payments",
            ["topics.blacklist"] = "test,internal"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal("orders.*", config.TopicsPattern);
        Assert.Equal(OrdersPayments, config.TopicsWhitelist);
        Assert.Equal(TestInternal, config.TopicsBlacklist);
    }

    [Fact]
    public void FromDictionary_ShouldParseGroupFiltering()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["groups"] = "prod-.*",
            ["groups.whitelist"] = "app-1,app-2",
            ["groups.blacklist"] = "test-group"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal("prod-.*", config.GroupsPattern);
        Assert.Equal(App1App2, config.GroupsWhitelist);
        Assert.Single(config.GroupsBlacklist);
    }

    [Fact]
    public void FromDictionary_ShouldParsePerformanceSettings()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["tasks.max"] = "8",
            ["consumer.poll.timeout.ms"] = "5000",
            ["producer.batch.size"] = "32768",
            ["fetch.max.bytes"] = "104857600"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal(8, config.TasksMax);
        Assert.Equal(5000, config.ConsumerPollTimeoutMs);
        Assert.Equal(32768, config.ProducerBatchSize);
        Assert.Equal(104857600, config.FetchMaxBytes);
    }

    [Fact]
    public void FromDictionary_ShouldParseBooleanSettings()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["sync.topic.configs.enabled"] = "false",
            ["sync.topic.acls.enabled"] = "true",
            ["emit.heartbeats.enabled"] = "false",
            ["emit.checkpoints.enabled"] = "false"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.False(config.SyncTopicConfigs);
        Assert.True(config.SyncTopicAcls);
        Assert.False(config.EmitHeartbeats);
        Assert.False(config.EmitCheckpoints);
    }

    [Fact]
    public void FromDictionary_ShouldParseSecuritySettings()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["source.security.protocol"] = "SASL_SSL",
            ["source.sasl.mechanism"] = "PLAIN",
            ["source.sasl.username"] = "user",
            ["source.sasl.password"] = "secret",
            ["target.security.protocol"] = "SSL",
            ["target.sasl.mechanism"] = "SCRAM-SHA-256"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal("SASL_SSL", config.SourceSecurityProtocol);
        Assert.Equal("PLAIN", config.SourceSaslMechanism);
        Assert.Equal("user", config.SourceSaslUsername);
        Assert.Equal("secret", config.SourceSaslPassword);
        Assert.Equal("SSL", config.TargetSecurityProtocol);
        Assert.Equal("SCRAM-SHA-256", config.TargetSaslMechanism);
    }

    [Fact]
    public void FromDictionary_ShouldParseReplicationPolicySettings()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["replication.policy.class"] = "Kuestenlogik.Surgewave.Connect.Mirror.Policies.IdentityReplicationPolicy",
            ["replication.policy.separator"] = "-"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal("Kuestenlogik.Surgewave.Connect.Mirror.Policies.IdentityReplicationPolicy", config.ReplicationPolicyClass);
        Assert.Equal("-", config.ReplicationPolicySeparator);
    }

    [Fact]
    public void FromDictionary_ShouldParseIntervalSettings()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["heartbeats.interval.ms"] = "2000",
            ["checkpoints.interval.ms"] = "120000",
            ["offset.sync.interval.ms"] = "90000",
            ["topic.refresh.interval.ms"] = "60000",
            ["group.refresh.interval.ms"] = "120000"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal(2000, config.HeartbeatIntervalMs);
        Assert.Equal(120000, config.CheckpointIntervalMs);
        Assert.Equal(90000, config.OffsetSyncIntervalMs);
        Assert.Equal(60000, config.TopicRefreshIntervalMs);
        Assert.Equal(120000, config.GroupRefreshIntervalMs);
    }

    [Fact]
    public void FromDictionary_ShouldParseTopicNames()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["heartbeats.topic"] = "custom.heartbeats",
            ["checkpoints.topic"] = "custom.checkpoints"
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal("custom.heartbeats", config.HeartbeatsTopic);
        Assert.Equal("custom.checkpoints", config.CheckpointsTopic);
    }

    [Fact]
    public void FromDictionary_ShouldHandleEmptyWhitelist()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["topics.whitelist"] = ""
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Empty(config.TopicsWhitelist);
    }

    [Fact]
    public void FromDictionary_ShouldTrimWhitelistItems()
    {
        var dict = new Dictionary<string, string>
        {
            ["source.bootstrap.servers"] = "localhost:9092",
            ["target.bootstrap.servers"] = "remote:9092",
            ["topics.whitelist"] = " orders , payments , users "
        };

        var config = MirrorMakerConfig.FromDictionary(dict);

        Assert.Equal(OrdersPaymentsUsers, config.TopicsWhitelist);
    }
}
