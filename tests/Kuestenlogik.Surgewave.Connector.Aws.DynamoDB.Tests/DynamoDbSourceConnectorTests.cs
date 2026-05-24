using Xunit;
using Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB.Tests;

public class DynamoDbSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new DynamoDbSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsDynamoDbSourceTask()
    {
        var connector = new DynamoDbSourceConnector();
        Assert.Equal(typeof(DynamoDbSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new DynamoDbSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.StreamArnConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.RegionConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.AccessKeyConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.SecretKeyConfig);
    }

    [Fact]
    public void Config_ContainsOptionalDefinitions()
    {
        var connector = new DynamoDbSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.EndpointConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.TopicPatternConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.ShardIteratorTypeConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.PollIntervalMsConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.BatchMaxRecordsConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.StartFromBeginningConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Start_WithStreamArn_Succeeds()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingStreamArn_ThrowsArgumentException()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.RegionConfig] = "us-east-1"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyStreamArn_ThrowsArgumentException()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = ""
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithCustomRegion_Succeeds()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:eu-west-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.RegionConfig] = "eu-west-1"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCredentials_Succeeds()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.AccessKeyConfig] = "AKIAIOSFODNN7EXAMPLE",
            [DynamoDbConnectorConfig.SecretKeyConfig] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomEndpoint_Succeeds()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.EndpointConfig] = "http://localhost:4566"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartFromBeginning_Succeeds()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.StartFromBeginningConfig] = "true"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithShardIteratorType_Succeeds()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.ShardIteratorTypeConfig] = DynamoDbConnectorConfig.ShardIteratorTrimHorizon
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomTopicPattern_Succeeds()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.TopicPatternConfig] = "cdc.${table}"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000",
            [DynamoDbConnectorConfig.PollIntervalMsConfig] = "1000"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        // Single task for DynamoDB Streams
        Assert.Single(taskConfigs);
        Assert.Equal("1000", taskConfigs[0][DynamoDbConnectorConfig.PollIntervalMsConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new DynamoDbSourceConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.StreamArnConfig] = "arn:aws:dynamodb:us-east-1:123456789012:table/TestTable/stream/2024-01-01T00:00:00.000"
        };
        connector.Start(config);

        var exception = Record.Exception(() =>
        {
            connector.Stop();
            connector.Stop();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Config_HasDefaultRegion()
    {
        var connector = new DynamoDbSourceConnector();
        var regionKey = connector.Config.Keys.First(k => k.Name == DynamoDbConnectorConfig.RegionConfig);

        Assert.Equal(DynamoDbConnectorConfig.DefaultRegion, regionKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultTopicPattern()
    {
        var connector = new DynamoDbSourceConnector();
        var patternKey = connector.Config.Keys.First(k => k.Name == DynamoDbConnectorConfig.TopicPatternConfig);

        Assert.Equal(DynamoDbConnectorConfig.DefaultTopicPattern, patternKey.DefaultValue);
    }
}
