using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb.Tests;

public class CosmosDbSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsCosmosDbSourceTask()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Equal(typeof(CosmosDbSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesConnectionStringConfig()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.ConnectionStringConfig);
    }

    [Fact]
    public void Config_DefinesEndpointConfig()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void Config_DefinesDatabaseConfig()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.DatabaseConfig);
    }

    [Fact]
    public void Config_DefinesContainerConfig()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.ContainerConfig);
    }

    [Fact]
    public void Config_DefinesChangeFeedConfigs()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.ChangeFeedStartFromConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.ChangeFeedMaxItemsConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.ChangeFeedPollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesLeaseConfigs()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.LeaseContainerConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.LeaseContainerPrefixConfig);
    }

    [Fact]
    public void Config_DefinesTopicPatternConfig()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_DefinesIncludeMetadataConfig()
    {
        var connector = new CosmosDbSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CosmosDbConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Start_ThrowsWhenNoConnectionProvided()
    {
        var connector = new CosmosDbSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CosmosDbConnectorConfig.ConnectionStringConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenDatabaseMissing()
    {
        var connector = new CosmosDbSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CosmosDbConnectorConfig.DatabaseConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenContainerMissing()
    {
        var connector = new CosmosDbSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CosmosDbConnectorConfig.ContainerConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsEndpointAndKey()
    {
        var connector = new CosmosDbSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.EndpointConfig] = "https://test.documents.azure.com:443/",
            [CosmosDbConnectorConfig.AccountKeyConfig] = "test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new CosmosDbSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer"
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
        var connector = new CosmosDbSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedTopicPatternDefault()
    {
        var connector = new CosmosDbSourceConnector();
        var topicPatternKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.TopicPatternConfig);
        Assert.Equal(CosmosDbConnectorConfig.DefaultTopicPattern, topicPatternKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedStartFromDefault()
    {
        var connector = new CosmosDbSourceConnector();
        var startFromKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.ChangeFeedStartFromConfig);
        Assert.Equal(CosmosDbConnectorConfig.StartFromNow, startFromKey.DefaultValue);
    }

    [Fact]
    public void Config_ConnectionStringIsPasswordType()
    {
        var connector = new CosmosDbSourceConnector();
        var connStringKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.ConnectionStringConfig);
        Assert.Equal(ConfigType.Password, connStringKey.Type);
    }

    [Fact]
    public void Config_AccountKeyIsPasswordType()
    {
        var connector = new CosmosDbSourceConnector();
        var accountKeyKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.AccountKeyConfig);
        Assert.Equal(ConfigType.Password, accountKeyKey.Type);
    }

    [Fact]
    public void Config_HasExpectedMaxItemsDefault()
    {
        var connector = new CosmosDbSourceConnector();
        var maxItemsKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.ChangeFeedMaxItemsConfig);
        Assert.Equal(CosmosDbConnectorConfig.DefaultChangeFeedMaxItems, maxItemsKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new CosmosDbSourceConnector();
        var pollIntervalKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.ChangeFeedPollIntervalMsConfig);
        Assert.Equal((int)CosmosDbConnectorConfig.DefaultPollIntervalMs, pollIntervalKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedLeaseContainerPrefixDefault()
    {
        var connector = new CosmosDbSourceConnector();
        var leasePrefixKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.LeaseContainerPrefixConfig);
        Assert.Equal(CosmosDbConnectorConfig.DefaultLeaseContainerPrefix, leasePrefixKey.DefaultValue);
    }

    [Fact]
    public void Config_IncludeMetadataDefaultsToTrue()
    {
        var connector = new CosmosDbSourceConnector();
        var includeMetadataKey = connector.Config.Keys.First(k => k.Name == CosmosDbConnectorConfig.IncludeMetadataConfig);
        Assert.Equal(true, includeMetadataKey.DefaultValue);
    }
}
