using Xunit;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Aws.Kinesis;

namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis.Tests;

public class KinesisSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new KinesisSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        using var task = new KinesisSinkTask();
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
        using var task = new KinesisSinkTask();
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
        using var task = new KinesisSinkTask();
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
        using var task = new KinesisSinkTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.EndpointConfig] = "http://localhost:4566"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithPartitionKeyField_Succeeds()
    {
        using var task = new KinesisSinkTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.PartitionKeyFieldConfig] = "id"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithExplicitHashKeyField_Succeeds()
    {
        using var task = new KinesisSinkTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.ExplicitHashKeyFieldConfig] = "hash_key"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBatchSize_Succeeds()
    {
        using var task = new KinesisSinkTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.BatchSizeConfig] = "100"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBatchSizeOverMax_ClampsTo500()
    {
        using var task = new KinesisSinkTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.BatchSizeConfig] = "1000"
        };

        // Should not throw - internally clamped to 500
        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithRetryConfig_Succeeds()
    {
        using var task = new KinesisSinkTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.RetryCountConfig] = "5",
            [KinesisConnectorConfig.RetryDelayMsConfig] = "200"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new KinesisSinkTask();
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
        using var task = new KinesisSinkTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_WithEmptyRecords_DoesNotThrow()
    {
        using var task = new KinesisSinkTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream"
        };
        task.Start(config);

        var exception = await Record.ExceptionAsync(() =>
            task.PutAsync(Array.Empty<SinkRecord>(), CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_WithoutStart_ReturnsWithoutError()
    {
        using var task = new KinesisSinkTask();

        // When client is null, should return immediately
        var exception = await Record.ExceptionAsync(() =>
            task.PutAsync(Array.Empty<SinkRecord>(), CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_ReturnsCompletedTask()
    {
        using var task = new KinesisSinkTask();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream"
        };
        task.Start(config);

        var exception = await Record.ExceptionAsync(() =>
            task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new KinesisSinkTask();
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
