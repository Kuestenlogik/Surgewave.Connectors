using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Akka.Tests;

public class AkkaSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        using var task = new AkkaSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_CreatesActorSystem()
    {
        using var task = new AkkaSinkTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_WithCustomSystemName_Succeeds()
    {
        using var task = new AkkaSinkTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorSystemNameConfig] = "custom-system",
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesAskTimeoutMs()
    {
        using var task = new AkkaSinkTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor",
            [AkkaConnectorConfig.AskTimeoutMsConfig] = "10000"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesTellOnly()
    {
        using var task = new AkkaSinkTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor",
            [AkkaConnectorConfig.TellOnlyConfig] = "false"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesBatchSize()
    {
        using var task = new AkkaSinkTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor",
            [AkkaConnectorConfig.BatchSizeConfig] = "16"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public async Task PutAsync_WithNullActorSystem_ReturnsImmediately()
    {
        using var task = new AkkaSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = System.Text.Encoding.UTF8.GetBytes("test") }
        };

        await task.PutAsync(records, CancellationToken.None);
    }

    [Fact]
    public async Task PutAsync_WithEmptyRecords_ReturnsImmediately()
    {
        using var task = new AkkaSinkTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor"
        };

        task.Start(config);
        await task.PutAsync([], CancellationToken.None);
        task.Stop();
    }

    [Fact]
    public async Task FlushAsync_ReturnsCompletedTask()
    {
        using var task = new AkkaSinkTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor"
        };

        task.Start(config);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        task.Stop();
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new AkkaSinkTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor"
        };

        task.Start(config);
        task.Stop();
        task.Stop();
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new AkkaSinkTask();
        var config = new Dictionary<string, string>
        {
            [AkkaConnectorConfig.ActorPathConfig] = "/user/processor"
        };

        task.Start(config);
        task.Dispose();
        task.Dispose();
    }
}
