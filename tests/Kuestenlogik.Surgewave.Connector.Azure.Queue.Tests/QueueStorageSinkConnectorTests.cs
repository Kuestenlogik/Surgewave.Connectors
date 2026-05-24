using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Queue.Tests;

public class QueueStorageSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSinkTaskType()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Equal(typeof(QueueStorageSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsConnectionStringConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.ConnectionStringConfig);
    }

    [Fact]
    public void Config_ContainsAccountNameConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.AccountNameConfig);
    }

    [Fact]
    public void Config_ContainsAccountKeyConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.AccountKeyConfig);
    }

    [Fact]
    public void Config_ContainsEndpointConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void Config_ContainsQueueNameConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.QueueNameConfig);
    }

    [Fact]
    public void Config_ContainsTopicsConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_ContainsTimeToLiveSecondsConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.TimeToLiveSecondsConfig);
    }

    [Fact]
    public void Config_ContainsBatchSizeConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_ContainsBase64EncodeConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.Base64EncodeConfig);
    }

    [Fact]
    public void Config_ContainsAutoCreateQueueConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.AutoCreateQueueConfig);
    }

    [Fact]
    public void Config_ContainsMaxRetryCountConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void Config_ContainsRetryDelayMsConfig()
    {
        var connector = new QueueStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == QueueStorageConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Config_ConnectionStringIsHighImportance()
    {
        var connector = new QueueStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.ConnectionStringConfig);
        Assert.Equal(Importance.High, key.Importance);
    }

    [Fact]
    public void Config_ConnectionStringIsPassword()
    {
        var connector = new QueueStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.ConnectionStringConfig);
        Assert.Equal(ConfigType.Password, key.Type);
    }

    [Fact]
    public void Config_QueueNameIsHighImportance()
    {
        var connector = new QueueStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.QueueNameConfig);
        Assert.Equal(Importance.High, key.Importance);
    }

    [Fact]
    public void Config_TopicsIsHighImportance()
    {
        var connector = new QueueStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, key.Importance);
    }

    [Fact]
    public void Config_TimeToLiveHasDefaultValue()
    {
        var connector = new QueueStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.TimeToLiveSecondsConfig);
        Assert.Equal(QueueStorageConnectorConfig.DefaultTimeToLiveSeconds, key.DefaultValue);
    }

    [Fact]
    public void Config_BatchSizeHasDefaultValue()
    {
        var connector = new QueueStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == QueueStorageConnectorConfig.BatchSizeConfig);
        Assert.Equal(QueueStorageConnectorConfig.DefaultBatchSize, key.DefaultValue);
    }

    [Fact]
    public void Start_WithConnectionString_Succeeds()
    {
        var connector = new QueueStorageSinkConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TopicsConfig] = "topic1,topic2"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithAccountNameAndKey_Succeeds()
    {
        var connector = new QueueStorageSinkConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.AccountNameConfig] = "testaccount",
            [QueueStorageConnectorConfig.AccountKeyConfig] = "dGVzdGtleQ==",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TopicsConfig] = "topic1"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithoutConnectionOrAccountName_ThrowsArgumentException()
    {
        var connector = new QueueStorageSinkConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TopicsConfig] = "topic1"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithoutQueueName_ThrowsArgumentException()
    {
        var connector = new QueueStorageSinkConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.TopicsConfig] = "topic1"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithoutTopics_ThrowsArgumentException()
    {
        var connector = new QueueStorageSinkConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyTopics_ThrowsArgumentException()
    {
        var connector = new QueueStorageSinkConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TopicsConfig] = ""
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new QueueStorageSinkConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TopicsConfig] = "topic1,topic2"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        connector.Stop();
    }

    [Fact]
    public void TaskConfigs_ConfigContainsAllOriginalSettings()
    {
        var connector = new QueueStorageSinkConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TopicsConfig] = "topic1,topic2",
            [QueueStorageConnectorConfig.TimeToLiveSecondsConfig] = "3600"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);
        var taskConfig = taskConfigs[0];

        Assert.Equal(config[QueueStorageConnectorConfig.ConnectionStringConfig], taskConfig[QueueStorageConnectorConfig.ConnectionStringConfig]);
        Assert.Equal("test-queue", taskConfig[QueueStorageConnectorConfig.QueueNameConfig]);
        Assert.Equal("topic1,topic2", taskConfig[QueueStorageConnectorConfig.TopicsConfig]);
        Assert.Equal("3600", taskConfig[QueueStorageConnectorConfig.TimeToLiveSecondsConfig]);
        connector.Stop();
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new QueueStorageSinkConnector();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TopicsConfig] = "topic1"
        };

        connector.Start(config);
        connector.Stop();
        connector.Stop(); // Should not throw
    }
}
