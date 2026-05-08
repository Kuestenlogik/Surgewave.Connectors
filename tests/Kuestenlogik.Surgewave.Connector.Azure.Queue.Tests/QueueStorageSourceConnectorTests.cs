using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Queue.Tests;

public class QueueStorageSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSourceTaskType()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Equal(typeof(QueueStorageSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsConnectionStringConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.ConnectionStringConfig);
    }

    [Fact]
    public void Config_ContainsAccountNameConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.AccountNameConfig);
    }

    [Fact]
    public void Config_ContainsAccountKeyConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.AccountKeyConfig);
    }

    [Fact]
    public void Config_ContainsEndpointConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void Config_ContainsQueueNameConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.QueueNameConfig);
    }

    [Fact]
    public void Config_ContainsTopicPatternConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_ContainsPollIntervalMsConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_ContainsMaxMessagesPerPollConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.MaxMessagesPerPollConfig);
    }

    [Fact]
    public void Config_ContainsVisibilityTimeoutSecondsConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.VisibilityTimeoutSecondsConfig);
    }

    [Fact]
    public void Config_ContainsDeleteAfterReadConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.DeleteAfterReadConfig);
    }

    [Fact]
    public void Config_ContainsBase64DecodeConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.Base64DecodeConfig);
    }

    [Fact]
    public void Config_ContainsIncludeMetadataConfig()
    {
        var connector = new QueueStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_ConnectionStringIsHighImportance()
    {
        var connector = new QueueStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.ConnectionStringConfig);
        Assert.Equal(Importance.High, key.Importance);
    }

    [Fact]
    public void Config_ConnectionStringIsPassword()
    {
        var connector = new QueueStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.ConnectionStringConfig);
        Assert.Equal(ConfigType.Password, key.Type);
    }

    [Fact]
    public void Config_QueueNameIsHighImportance()
    {
        var connector = new QueueStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.QueueNameConfig);
        Assert.Equal(Importance.High, key.Importance);
    }

    [Fact]
    public void Config_TopicPatternHasDefaultValue()
    {
        var connector = new QueueStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.TopicPatternConfig);
        Assert.Equal(QueueStorageConnectorConfig.DefaultTopicPattern, key.DefaultValue);
    }

    [Fact]
    public void Start_WithConnectionString_Succeeds()
    {
        var connector = new QueueStorageSourceConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithAccountNameAndKey_Succeeds()
    {
        var connector = new QueueStorageSourceConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.AccountNameConfig] = "testaccount",
            [QueueStorageConnectorConfig.AccountKeyConfig] = "dGVzdGtleQ==",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithoutConnectionOrAccountName_ThrowsArgumentException()
    {
        var connector = new QueueStorageSourceConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithoutQueueName_ThrowsArgumentException()
    {
        var connector = new QueueStorageSourceConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new QueueStorageSourceConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        connector.Stop();
    }

    [Fact]
    public void TaskConfigs_ConfigContainsAllOriginalSettings()
    {
        var connector = new QueueStorageSourceConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TopicPatternConfig] = "custom.${queue}"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);
        var taskConfig = taskConfigs[0];

        Assert.Equal(config[QueueStorageConnectorConfig.ConnectionStringConfig], taskConfig[QueueStorageConnectorConfig.ConnectionStringConfig]);
        Assert.Equal("test-queue", taskConfig[QueueStorageConnectorConfig.QueueNameConfig]);
        Assert.Equal("custom.${queue}", taskConfig[QueueStorageConnectorConfig.TopicPatternConfig]);
        connector.Stop();
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new QueueStorageSourceConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        connector.Start(config);
        connector.Stop();
        connector.Stop(); // Should not throw
    }
}
