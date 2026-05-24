namespace Kuestenlogik.Surgewave.Connector.Akka.Tests;

public class AkkaSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        using var task = new AkkaSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_CreatesActorSystem()
    {
        using var task = new AkkaSourceTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/test-receiver"
        };

        task.Start(config);
        Assert.NotNull(task.ActorSystem);
        Assert.NotNull(task.ReceiverActor);
        task.Stop();
    }

    [Fact]
    public void Start_WithCustomSystemName_Succeeds()
    {
        using var task = new AkkaSourceTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorSystemNameConfig] = "custom-system",
            [AkkaConnectorConfig.ActorPathConfig] = "/user/test-receiver"
        };

        task.Start(config);
        Assert.NotNull(task.ActorSystem);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesPollTimeoutMs()
    {
        using var task = new AkkaSourceTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/test-receiver",
            [AkkaConnectorConfig.PollTimeoutMsConfig] = "5000"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesMaxMessagesPerPoll()
    {
        using var task = new AkkaSourceTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/test-receiver",
            [AkkaConnectorConfig.MaxMessagesPerPollConfig] = "50"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public async Task PollAsync_BeforeStart_ReturnsEmptyList()
    {
        using var task = new AkkaSourceTask();
        var result = await task.PollAsync(CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public async Task PollAsync_AfterStop_ReturnsEmptyList()
    {
        using var task = new AkkaSourceTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/test-receiver"
        };

        task.Start(config);
        task.Stop();

        var result = await task.PollAsync(CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public async Task CommitAsync_Succeeds()
    {
        using var task = new AkkaSourceTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/test-receiver"
        };

        task.Start(config);
        await task.CommitAsync(CancellationToken.None);
        task.Stop();
    }

    [Fact]
    public void Stop_ClearsActorSystem()
    {
        using var task = new AkkaSourceTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/test-receiver"
        };

        task.Start(config);
        task.Stop();
        Assert.Null(task.ActorSystem);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new AkkaSourceTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/test-receiver"
        };

        task.Start(config);
        task.Dispose();
        task.Dispose();
    }
}
