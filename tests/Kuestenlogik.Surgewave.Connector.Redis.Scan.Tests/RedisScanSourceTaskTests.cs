using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Redis.Scan.Tests;

/// <summary>
/// The task opens its Redis connection at the end of <c>Start</c>, so every configuration
/// mistake has to be reported before a socket is ever opened - that is the part a worker can
/// diagnose without a reachable server.
/// </summary>
public class RedisScanSourceTaskTests
{
    [Fact]
    public void Start_WithoutTopic_FailsBeforeConnecting()
    {
        using var task = new RedisScanSourceTask();
        task.Initialize(new TaskContext());

        var config = new Dictionary<string, string>
        {
            [RedisScanConnectorConfig.Pattern] = "user:*"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public void Start_WithNonNumericDatabase_FailsBeforeConnecting()
    {
        using var task = new RedisScanSourceTask();
        task.Initialize(new TaskContext());

        var config = Config();
        config[RedisScanConnectorConfig.Database] = "primary";

        Assert.Throws<FormatException>(() => task.Start(config));
    }

    [Fact]
    public void Start_WithNonNumericBatchSize_FailsBeforeConnecting()
    {
        using var task = new RedisScanSourceTask();
        task.Initialize(new TaskContext());

        var config = Config();
        config[RedisScanConnectorConfig.BatchSize] = "all-of-them";

        Assert.Throws<FormatException>(() => task.Start(config));
    }

    [Fact]
    public void Start_WithNonNumericPollInterval_FailsBeforeConnecting()
    {
        using var task = new RedisScanSourceTask();
        task.Initialize(new TaskContext());

        var config = Config();
        config[RedisScanConnectorConfig.PollIntervalMs] = "hourly";

        Assert.Throws<FormatException>(() => task.Start(config));
    }

    private static Dictionary<string, string> Config() => new()
    {
        [RedisScanConnectorConfig.ConnectionString] = "localhost:6379",
        [RedisScanConnectorConfig.Topic] = "redis-keys",
        [RedisScanConnectorConfig.Pattern] = "user:*"
    };
}
