using Xunit;
using Kuestenlogik.Surgewave.Connector.Aws.Kinesis;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis.Tests;

public class KinesisSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new KinesisSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsKinesisSourceTask()
    {
        var connector = new KinesisSourceConnector();
        Assert.Equal(typeof(KinesisSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new KinesisSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.StreamNameConfig);
    }

    [Fact]
    public void Config_ContainsOptionalDefinitions()
    {
        var connector = new KinesisSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.RegionConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.AccessKeyConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.SecretKeyConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.EndpointConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.TopicPatternConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.ShardIteratorTypeConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.PollIntervalMsConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.BatchMaxRecordsConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.StartFromBeginningConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.StartTimestampConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Start_WithStreamName_Succeeds()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingStreamName_ThrowsArgumentException()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.RegionConfig] = "us-east-1"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyStreamName_ThrowsArgumentException()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = ""
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithCustomRegion_Succeeds()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.RegionConfig] = "eu-west-1"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCredentials_Succeeds()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.AccessKeyConfig] = "AKIAIOSFODNN7EXAMPLE",
            [KinesisConnectorConfig.SecretKeyConfig] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomEndpoint_Succeeds()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.EndpointConfig] = "http://localhost:4566"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartFromBeginning_Succeeds()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.StartFromBeginningConfig] = "true"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithShardIteratorType_Succeeds()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.ShardIteratorTypeConfig] = KinesisConnectorConfig.ShardIteratorTrimHorizon
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomTopicPattern_Succeeds()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicPatternConfig] = "events.${stream}"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartTimestamp_Succeeds()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.StartTimestampConfig] = "2024-01-01T00:00:00Z"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.PollIntervalMsConfig] = "1000"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("1000", taskConfigs[0][KinesisConnectorConfig.PollIntervalMsConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new KinesisSourceConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream"
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
        var connector = new KinesisSourceConnector();
        var regionKey = connector.Config.Keys.First(k => k.Name == KinesisConnectorConfig.RegionConfig);

        Assert.Equal(KinesisConnectorConfig.DefaultRegion, regionKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultTopicPattern()
    {
        var connector = new KinesisSourceConnector();
        var patternKey = connector.Config.Keys.First(k => k.Name == KinesisConnectorConfig.TopicPatternConfig);

        Assert.Equal(KinesisConnectorConfig.DefaultTopicPattern, patternKey.DefaultValue);
    }
}
