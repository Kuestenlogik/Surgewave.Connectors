using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.Table;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Table.Tests;

public class TableStorageSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsTableStorageSinkTask()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Equal(typeof(TableStorageSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesConnectionStringConfig()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.ConnectionStringConfig);
    }

    [Fact]
    public void Config_DefinesAccountNameConfig()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.AccountNameConfig);
    }

    [Fact]
    public void Config_DefinesTableNameConfig()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.TableNameConfig);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesWriteModeConfig()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.WriteModeConfig);
    }

    [Fact]
    public void Config_DefinesKeyFieldConfigs()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.PartitionKeyFieldConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.RowKeyFieldConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesAutoCreateTableConfig()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.AutoCreateTableConfig);
    }

    [Fact]
    public void Config_DefinesRetryConfigs()
    {
        var connector = new TableStorageSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.MaxRetryCountConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenNoConnectionProvided()
    {
        var connector = new TableStorageSinkConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.TableNameConfig] = "testtable",
            [TableStorageConnectorConfig.TopicsConfig] = "test-topic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TableStorageConnectorConfig.ConnectionStringConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTableNameMissing()
    {
        var connector = new TableStorageSinkConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
            [TableStorageConnectorConfig.TopicsConfig] = "test-topic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TableStorageConnectorConfig.TableNameConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new TableStorageSinkConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
            [TableStorageConnectorConfig.TableNameConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TableStorageConnectorConfig.TopicsConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsAccountNameAndKey()
    {
        var connector = new TableStorageSinkConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.AccountNameConfig] = "testaccount",
            [TableStorageConnectorConfig.AccountKeyConfig] = "dGVzdGtleQ==",
            [TableStorageConnectorConfig.TableNameConfig] = "testtable",
            [TableStorageConnectorConfig.TopicsConfig] = "test-topic"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new TableStorageSinkConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
            [TableStorageConnectorConfig.TableNameConfig] = "testtable",
            [TableStorageConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        Assert.Equal("testtable", taskConfigs[0][TableStorageConnectorConfig.TableNameConfig]);
    }

    [Fact]
    public void Stop_CompletesWithoutError()
    {
        var connector = new TableStorageSinkConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
            [TableStorageConnectorConfig.TableNameConfig] = "testtable",
            [TableStorageConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_WriteModeHasCorrectDefault()
    {
        var connector = new TableStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.WriteModeConfig);
        Assert.Equal(TableStorageConnectorConfig.WriteModeUpsert, key.DefaultValue);
    }

    [Fact]
    public void Config_PartitionKeyFieldHasCorrectDefault()
    {
        var connector = new TableStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.PartitionKeyFieldConfig);
        Assert.Equal("partitionKey", key.DefaultValue);
    }

    [Fact]
    public void Config_RowKeyFieldHasCorrectDefault()
    {
        var connector = new TableStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.RowKeyFieldConfig);
        Assert.Equal("rowKey", key.DefaultValue);
    }

    [Fact]
    public void Config_BatchSizeHasCorrectDefault()
    {
        var connector = new TableStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.BatchSizeConfig);
        Assert.Equal(TableStorageConnectorConfig.DefaultBatchSize, key.DefaultValue);
    }

    [Fact]
    public void Config_AutoCreateTableDefaultsToFalse()
    {
        var connector = new TableStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.AutoCreateTableConfig);
        Assert.Equal(false, key.DefaultValue);
    }

    [Fact]
    public void Config_MaxRetryCountHasCorrectDefault()
    {
        var connector = new TableStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.MaxRetryCountConfig);
        Assert.Equal(TableStorageConnectorConfig.DefaultMaxRetryCount, key.DefaultValue);
    }

    [Fact]
    public void Config_RetryDelayHasCorrectDefault()
    {
        var connector = new TableStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.RetryDelayMsConfig);
        Assert.Equal((int)TableStorageConnectorConfig.DefaultRetryDelayMs, key.DefaultValue);
    }

    [Fact]
    public void Config_ConnectionStringIsPasswordType()
    {
        var connector = new TableStorageSinkConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.ConnectionStringConfig);
        Assert.Equal(ConfigType.Password, key.Type);
    }
}
