using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.Table;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Table.Tests;

public class TableStorageSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsTableStorageSourceTask()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Equal(typeof(TableStorageSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesConnectionStringConfig()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.ConnectionStringConfig);
    }

    [Fact]
    public void Config_DefinesAccountNameConfig()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.AccountNameConfig);
    }

    [Fact]
    public void Config_DefinesAccountKeyConfig()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.AccountKeyConfig);
    }

    [Fact]
    public void Config_DefinesTableNameConfig()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.TableNameConfig);
    }

    [Fact]
    public void Config_DefinesSourceConfigs()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.QueryFilterConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.SelectColumnsConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.PollIntervalMsConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.MaxEntitiesPerPollConfig);
    }

    [Fact]
    public void Config_DefinesIncrementalConfigs()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.IncrementalModeConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.IncrementalColumnConfig);
    }

    [Fact]
    public void Config_DefinesTopicPatternConfig()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_DefinesIncludeMetadataConfig()
    {
        var connector = new TableStorageSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == TableStorageConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Start_ThrowsWhenNoConnectionProvided()
    {
        var connector = new TableStorageSourceConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.TableNameConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TableStorageConnectorConfig.ConnectionStringConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTableNameMissing()
    {
        var connector = new TableStorageSourceConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TableStorageConnectorConfig.TableNameConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsAccountNameAndKey()
    {
        var connector = new TableStorageSourceConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.AccountNameConfig] = "testaccount",
            [TableStorageConnectorConfig.AccountKeyConfig] = "dGVzdGtleQ==",
            [TableStorageConnectorConfig.TableNameConfig] = "testtable"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new TableStorageSourceConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
            [TableStorageConnectorConfig.TableNameConfig] = "testtable"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        Assert.Equal("testtable", taskConfigs[0][TableStorageConnectorConfig.TableNameConfig]);
    }

    [Fact]
    public void Stop_CompletesWithoutError()
    {
        var connector = new TableStorageSourceConnector();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
            [TableStorageConnectorConfig.TableNameConfig] = "testtable"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedTopicPatternDefault()
    {
        var connector = new TableStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.TopicPatternConfig);
        Assert.Equal(TableStorageConnectorConfig.DefaultTopicPattern, key.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new TableStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.PollIntervalMsConfig);
        Assert.Equal((int)TableStorageConnectorConfig.DefaultPollIntervalMs, key.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedIncrementalModeDefault()
    {
        var connector = new TableStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.IncrementalModeConfig);
        Assert.Equal(TableStorageConnectorConfig.IncrementalModeNone, key.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxEntitiesDefault()
    {
        var connector = new TableStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.MaxEntitiesPerPollConfig);
        Assert.Equal(TableStorageConnectorConfig.DefaultMaxEntitiesPerPoll, key.DefaultValue);
    }

    [Fact]
    public void Config_IncludeMetadataDefaultsToTrue()
    {
        var connector = new TableStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.IncludeMetadataConfig);
        Assert.Equal(true, key.DefaultValue);
    }

    [Fact]
    public void Config_ConnectionStringIsPasswordType()
    {
        var connector = new TableStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.ConnectionStringConfig);
        Assert.Equal(ConfigType.Password, key.Type);
    }

    [Fact]
    public void Config_AccountKeyIsPasswordType()
    {
        var connector = new TableStorageSourceConnector();
        var key = connector.Config.Keys.First(k => k.Name == TableStorageConnectorConfig.AccountKeyConfig);
        Assert.Equal(ConfigType.Password, key.Type);
    }
}
