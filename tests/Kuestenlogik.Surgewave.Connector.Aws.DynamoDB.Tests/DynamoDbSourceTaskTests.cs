using Xunit;
using Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB.Tests;

public class DynamoDbSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new DynamoDbSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithStreamArn_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomRegion_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:eu-west-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.RegionConfig] = "eu-west-1"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCredentials_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.AccessKeyConfig] = "AKIAIOSFODNN7EXAMPLE",
            [DynamoDbConnectorConfig.SecretKeyConfig] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomEndpoint_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.EndpointConfig] = "http://localhost:4566"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithPollInterval_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.PollIntervalMsConfig] = "1000"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBatchMaxRecords_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.BatchMaxRecordsConfig] = "50"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartFromBeginning_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.StartFromBeginningConfig] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithShardIteratorTypeTrimHorizon_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.ShardIteratorTypeConfig] = DynamoDbConnectorConfig.ShardIteratorTrimHorizon
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithShardIteratorTypeLatest_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.ShardIteratorTypeConfig] = DynamoDbConnectorConfig.ShardIteratorLatest
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomTopicPattern_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.TopicPatternConfig] = "cdc.${table}"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithIncludeMetadataFalse_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.IncludeMetadataConfig] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomTableName_Succeeds()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.TableNameConfig] = "CustomTableName"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000"
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
        using var task = new DynamoDbSourceTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public async Task CommitAsync_ReturnsCompletedTask()
    {
        using var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000"
        };
        task.Start(config);

        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new DynamoDbSourceTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000"
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
