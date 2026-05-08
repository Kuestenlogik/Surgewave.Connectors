using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb.Tests;

public class CosmosDbSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsCosmosDbSinkTask()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Equal(typeof(CosmosDbSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesConnectionStringConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.ConnectionStringConfig);
    }

    [Fact]
    public void Config_DefinesEndpointConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void Config_DefinesDatabaseConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.DatabaseConfig);
    }

    [Fact]
    public void Config_DefinesContainerConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.ContainerConfig);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesWriteModeConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.WriteModeConfig);
    }

    [Fact]
    public void Config_DefinesPartitionKeyPathConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.PartitionKeyPathConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesAutoCreateContainerConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.AutoCreateContainerConfig);
    }

    [Fact]
    public void Config_DefinesThroughputConfig()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.ThroughputConfig);
    }

    [Fact]
    public void Config_DefinesRetryConfigs()
    {
        var connector = new CosmosDbSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.MaxRetryCountConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.MaxRetryWaitTimeMsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenNoConnectionProvided()
    {
        var connector = new CosmosDbSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer",
            [CosmosDbConnectorConfig.TopicsConfig] = "test-topic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CosmosDbConnectorConfig.ConnectionStringConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenDatabaseMissing()
    {
        var connector = new CosmosDbSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer",
            [CosmosDbConnectorConfig.TopicsConfig] = "test-topic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CosmosDbConnectorConfig.DatabaseConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenContainerMissing()
    {
        var connector = new CosmosDbSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.TopicsConfig] = "test-topic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CosmosDbConnectorConfig.ContainerConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new CosmosDbSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CosmosDbConnectorConfig.TopicsConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsEndpointAndKey()
    {
        var connector = new CosmosDbSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.EndpointConfig] = "https://test.documents.azure.com:443/",
            [CosmosDbConnectorConfig.AccountKeyConfig] = "test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer",
            [CosmosDbConnectorConfig.TopicsConfig] = "test-topic"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new CosmosDbSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer",
            [CosmosDbConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        Assert.Equal("testdb", taskConfigs[0][CosmosDbConnectorConfig.DatabaseConfig]);
        Assert.Equal("testcontainer", taskConfigs[0][CosmosDbConnectorConfig.ContainerConfig]);
    }

    [Fact]
    public void Stop_CompletesWithoutError()
    {
        var connector = new CosmosDbSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer",
            [CosmosDbConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_WriteModeHasCorrectDefault()
    {
        var connector = new CosmosDbSinkConnector();
        var writeModeKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.WriteModeConfig);
        Assert.Equal(CosmosDbConnectorConfig.WriteModeUpsert, writeModeKey.DefaultValue);
    }

    [Fact]
    public void Config_PartitionKeyPathHasCorrectDefault()
    {
        var connector = new CosmosDbSinkConnector();
        var pkPathKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.PartitionKeyPathConfig);
        Assert.Equal("/id", pkPathKey.DefaultValue);
    }

    [Fact]
    public void Config_BatchSizeHasCorrectDefault()
    {
        var connector = new CosmosDbSinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.BatchSizeConfig);
        Assert.Equal(CosmosDbConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_AutoCreateContainerDefaultsToFalse()
    {
        var connector = new CosmosDbSinkConnector();
        var autoCreateKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.AutoCreateContainerConfig);
        Assert.Equal(false, autoCreateKey.DefaultValue);
    }

    [Fact]
    public void Config_ThroughputHasCorrectDefault()
    {
        var connector = new CosmosDbSinkConnector();
        var throughputKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.ThroughputConfig);
        Assert.Equal(CosmosDbConnectorConfig.DefaultThroughput, throughputKey.DefaultValue);
    }

    [Fact]
    public void Config_ConnectionStringIsPasswordType()
    {
        var connector = new CosmosDbSinkConnector();
        var connStringKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.ConnectionStringConfig);
        Assert.Equal(ConfigType.Password, connStringKey.Type);
    }

    [Fact]
    public void Config_MaxRetryCountHasCorrectDefault()
    {
        var connector = new CosmosDbSinkConnector();
        var retryCountKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.MaxRetryCountConfig);
        Assert.Equal(CosmosDbConnectorConfig.DefaultMaxRetryCount, retryCountKey.DefaultValue);
    }

    [Fact]
    public void Config_MaxRetryWaitTimeHasCorrectDefault()
    {
        var connector = new CosmosDbSinkConnector();
        var retryWaitKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.MaxRetryWaitTimeMsConfig);
        Assert.Equal((int)CosmosDbConnectorConfig.DefaultMaxRetryWaitTimeMs, retryWaitKey.DefaultValue);
    }

    [Fact]
    public void Config_IdFieldHasCorrectDefault()
    {
        var connector = new CosmosDbSinkConnector();
        var idFieldKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.IdFieldConfig);
        Assert.Equal("id", idFieldKey.DefaultValue);
    }
}
