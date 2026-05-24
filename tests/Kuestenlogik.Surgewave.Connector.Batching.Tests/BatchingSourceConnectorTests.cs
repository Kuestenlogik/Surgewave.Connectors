using Xunit;
using Kuestenlogik.Surgewave.Connector.Batching;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Batching.Tests;

public class BatchingSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        using var connector = new BatchingSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsBatchingSourceTask()
    {
        using var connector = new BatchingSourceConnector();
        Assert.Equal(typeof(BatchingSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsTopicsConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_ContainsBatchMaxMessagesConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.BatchMaxMessagesConfig);
    }

    [Fact]
    public void Config_ContainsBatchMaxBytesConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.BatchMaxBytesConfig);
    }

    [Fact]
    public void Config_ContainsBatchTimeoutMsConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.BatchTimeoutMsConfig);
    }

    [Fact]
    public void Config_ContainsBatchFormatConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.BatchFormatConfig);
    }

    [Fact]
    public void Config_ContainsKeyStrategyConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.KeyStrategyConfig);
    }

    [Fact]
    public void Config_ContainsIncludeMetadataConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_ContainsSeparatorConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.SeparatorConfig);
    }

    [Fact]
    public void Config_ContainsFlushOnKeyChangeConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.FlushOnKeyChangeConfig);
    }

    [Fact]
    public void Config_ContainsCompressionConfig()
    {
        using var connector = new BatchingSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == BatchingConnectorConfig.CompressionConfig);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithMissingTopics_ThrowsArgumentException()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithInvalidBatchFormat_ThrowsArgumentException()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchFormatConfig] = "invalid-format"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithInvalidKeyStrategy_ThrowsArgumentException()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.KeyStrategyConfig] = "invalid-strategy"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithInvalidCompression_ThrowsArgumentException()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.CompressionConfig] = "invalid-compression"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithJsonArrayFormat_Succeeds()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatJsonArray
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithJsonLinesFormat_Succeeds()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatJsonLines
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithRawFormat_Succeeds()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchFormatConfig] = BatchingConnectorConfig.FormatRaw
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithGzipCompression_Succeeds()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.CompressionConfig] = BatchingConnectorConfig.CompressionGzip
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void TaskConfigs_ReturnsConfiguredValues()
    {
        using var connector = new BatchingSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [BatchingConnectorConfig.TopicsConfig] = "test-topic",
            [BatchingConnectorConfig.BatchMaxMessagesConfig] = "50"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][BatchingConnectorConfig.TopicsConfig]);
        Assert.Equal("50", taskConfigs[0][BatchingConnectorConfig.BatchMaxMessagesConfig]);

        connector.Stop();
    }
}
