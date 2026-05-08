using Xunit;
using Kuestenlogik.Surgewave.Connector.Aws.Kinesis;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Kinesis.Tests;

public class KinesisSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new KinesisSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsKinesisSinkTask()
    {
        var connector = new KinesisSinkConnector();
        Assert.Equal(typeof(KinesisSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new KinesisSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.StreamNameConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_ContainsOptionalDefinitions()
    {
        var connector = new KinesisSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.RegionConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.AccessKeyConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.SecretKeyConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.EndpointConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.PartitionKeyFieldConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.ExplicitHashKeyFieldConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.BatchSizeConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.RetryCountConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == KinesisConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingStreamName_ThrowsArgumentException()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.TopicsConfig] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyStreamName_ThrowsArgumentException()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingTopics_ThrowsArgumentException()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyTopics_ThrowsArgumentException()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = ""
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithCustomRegion_Succeeds()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic",
            [KinesisConnectorConfig.RegionConfig] = "eu-west-1"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCredentials_Succeeds()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic",
            [KinesisConnectorConfig.AccessKeyConfig] = "AKIAIOSFODNN7EXAMPLE",
            [KinesisConnectorConfig.SecretKeyConfig] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomEndpoint_Succeeds()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic",
            [KinesisConnectorConfig.EndpointConfig] = "http://localhost:4566"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithPartitionKeyField_Succeeds()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic",
            [KinesisConnectorConfig.PartitionKeyFieldConfig] = "id"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithExplicitHashKeyField_Succeeds()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic",
            [KinesisConnectorConfig.ExplicitHashKeyFieldConfig] = "hash_key"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBatchSize_Succeeds()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic",
            [KinesisConnectorConfig.BatchSizeConfig] = "100"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithRetryConfig_Succeeds()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic",
            [KinesisConnectorConfig.RetryCountConfig] = "5",
            [KinesisConnectorConfig.RetryDelayMsConfig] = "200"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic",
            [KinesisConnectorConfig.BatchSizeConfig] = "100"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("100", taskConfigs[0][KinesisConnectorConfig.BatchSizeConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new KinesisSinkConnector();
        var config = new Dictionary<string, string>
        {
            [KinesisConnectorConfig.StreamNameConfig] = "test-stream",
            [KinesisConnectorConfig.TopicsConfig] = "test-topic"
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
        var connector = new KinesisSinkConnector();
        var regionKey = connector.Config.Keys.First(k => k.Name == KinesisConnectorConfig.RegionConfig);

        Assert.Equal(KinesisConnectorConfig.DefaultRegion, regionKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultBatchSize()
    {
        var connector = new KinesisSinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == KinesisConnectorConfig.BatchSizeConfig);

        Assert.Equal(KinesisConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultRetryCount()
    {
        var connector = new KinesisSinkConnector();
        var retryCountKey = connector.Config.Keys.First(k => k.Name == KinesisConnectorConfig.RetryCountConfig);

        Assert.Equal(KinesisConnectorConfig.DefaultRetryCount, retryCountKey.DefaultValue);
    }
}
