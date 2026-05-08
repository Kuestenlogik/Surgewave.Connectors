using Xunit;
using Kuestenlogik.Surgewave.Connector.Aws.Kinesis;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis.Tests;

public class KinesisSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new KinesisSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithStreamName_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomRegion_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.RegionConfig] = "eu-west-1"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCredentials_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.AccessKeyConfig] = "AKIAIOSFODNN7EXAMPLE",
            [KinesisConnectorConfig.SecretKeyConfig] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomEndpoint_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.EndpointConfig] = "http://localhost:4566"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithPollInterval_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.PollIntervalMsConfig] = "1000"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBatchMaxRecords_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.BatchMaxRecordsConfig] = "50"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartFromBeginning_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.StartFromBeginningConfig] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithShardIteratorTypeTrimHorizon_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.ShardIteratorTypeConfig] = KinesisConnectorConfig.ShardIteratorTrimHorizon
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithShardIteratorTypeLatest_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.ShardIteratorTypeConfig] = KinesisConnectorConfig.ShardIteratorLatest
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomTopicPattern_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicPatternConfig] = "events.${stream}"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithIncludeMetadataFalse_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.IncludeMetadataConfig] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartTimestamp_Succeeds()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.StartTimestampConfig] = "2024-01-01T00:00:00Z"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream"
        };
        task.Start(config);

        var exception = Record.Exception(() =>
        {
            task.Stop();
            task.Stop();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new KinesisSourceTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public async Task CommitAsync_ReturnsCompletedTask()
    {
        using var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream"
        };
        task.Start(config);

        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new KinesisSourceTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream"
        };
        task.Start(config);

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }
}
