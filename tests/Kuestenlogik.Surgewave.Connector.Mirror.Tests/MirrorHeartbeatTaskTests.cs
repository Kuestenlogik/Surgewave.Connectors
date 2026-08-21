namespace Kuestenlogik.Surgewave.Connector.Mirror.Tests;

public class MirrorHeartbeatTaskTests
{
    private static Dictionary<string, string> BaseConfig() => new()
    {
        ["source.cluster.alias"] = "dc1",
        ["target.cluster.alias"] = "dc2",
        ["heartbeats.interval.ms"] = "0"
    };

    [Fact]
    public async Task PollAsync_WithoutConfiguredTopic_UsesReplicationPolicyTopic()
    {
        using var task = new MirrorHeartbeatTask();
        task.Start(BaseConfig());

        var records = await task.PollAsync(CancellationToken.None);

        var record = Assert.Single(records);
        Assert.Equal("dc1.heartbeats", record.Topic);
    }

    [Fact]
    public async Task PollAsync_WithConfiguredTopic_EmitsToConfiguredTopic()
    {
        using var task = new MirrorHeartbeatTask();
        var config = BaseConfig();
        config["heartbeats.topic"] = "ops.heartbeats";
        task.Start(config);

        var records = await task.PollAsync(CancellationToken.None);

        var record = Assert.Single(records);
        Assert.Equal("ops.heartbeats", record.Topic);
    }
}
