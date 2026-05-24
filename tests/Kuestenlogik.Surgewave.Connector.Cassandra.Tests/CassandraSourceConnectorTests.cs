using Kuestenlogik.Surgewave.Connector.Cassandra;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Cassandra.Tests;

public class CassandraSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new CassandraSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsCassandraSourceTask()
    {
        var connector = new CassandraSourceConnector();
        Assert.Equal(typeof(CassandraSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesContactPointsConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.ContactPointsConfig);
    }

    [Fact]
    public void Config_DefinesPortConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.PortConfig);
    }

    [Fact]
    public void Config_DefinesDatacenterConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.DatacenterConfig);
    }

    [Fact]
    public void Config_DefinesKeyspaceConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.KeyspaceConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesConsistencyLevelConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.ConsistencyLevelConfig);
    }

    [Fact]
    public void Config_DefinesSslEnabledConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.SslEnabledConfig);
    }

    [Fact]
    public void Config_DefinesModeConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.ModeConfig);
    }

    [Fact]
    public void Config_DefinesTableConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.TableConfig);
    }

    [Fact]
    public void Config_DefinesQueryConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.QueryConfig);
    }

    [Fact]
    public void Config_DefinesTopicPatternConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesMaxRowsPerPollConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.MaxRowsPerPollConfig);
    }

    [Fact]
    public void Config_DefinesIncludeMetadataConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_DefinesTimestampColumnConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.TimestampColumnConfig);
    }

    [Fact]
    public void Config_DefinesPartitionKeyColumnsConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.PartitionKeyColumnsConfig);
    }

    [Fact]
    public void Config_DefinesClusteringKeyColumnsConfig()
    {
        var connector = new CassandraSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.ClusteringKeyColumnsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenContactPointsMissing()
    {
        var connector = new CassandraSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CassandraConnectorConfig.ContactPointsConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenKeyspaceMissing()
    {
        var connector = new CassandraSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CassandraConnectorConfig.KeyspaceConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTableMissingInTableMode()
    {
        var connector = new CassandraSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.ModeConfig] = "table"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CassandraConnectorConfig.TableConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenQueryMissingInQueryMode()
    {
        var connector = new CassandraSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.ModeConfig] = "query"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CassandraConnectorConfig.QueryConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsValidTableModeConfig()
    {
        var connector = new CassandraSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TableConfig] = "testtable"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void Start_AcceptsValidQueryModeConfig()
    {
        var connector = new CassandraSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.ModeConfig] = "query",
            [CassandraConnectorConfig.QueryConfig] = "SELECT * FROM testtable"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new CassandraSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TableConfig] = "testtable"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        Assert.Equal("localhost", taskConfigs[0][CassandraConnectorConfig.ContactPointsConfig]);
        Assert.Equal("testkeyspace", taskConfigs[0][CassandraConnectorConfig.KeyspaceConfig]);
        Assert.Equal("testtable", taskConfigs[0][CassandraConnectorConfig.TableConfig]);
    }

    [Fact]
    public void Stop_CompletesWithoutError()
    {
        var connector = new CassandraSourceConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TableConfig] = "testtable"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedTopicPatternDefault()
    {
        var connector = new CassandraSourceConnector();
        var topicPatternKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.TopicPatternConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultTopicPattern, topicPatternKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedModeDefault()
    {
        var connector = new CassandraSourceConnector();
        var modeKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.ModeConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultMode, modeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedConsistencyDefault()
    {
        var connector = new CassandraSourceConnector();
        var consistencyKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.ConsistencyLevelConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultConsistencyLevel, consistencyKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPortDefault()
    {
        var connector = new CassandraSourceConnector();
        var portKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.PortConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultPort, portKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new CassandraSourceConnector();
        var pollIntervalKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.PollIntervalMsConfig);
        Assert.Equal((int)CassandraConnectorConfig.DefaultPollIntervalMs, pollIntervalKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRowsPerPollDefault()
    {
        var connector = new CassandraSourceConnector();
        var maxRowsKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.MaxRowsPerPollConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultMaxRowsPerPoll, maxRowsKey.DefaultValue);
    }

    [Fact]
    public void Config_IncludeMetadataDefaultsToTrue()
    {
        var connector = new CassandraSourceConnector();
        var includeMetadataKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.IncludeMetadataConfig);
        Assert.Equal(true, includeMetadataKey.DefaultValue);
    }

    [Fact]
    public void Config_SslEnabledDefaultsToFalse()
    {
        var connector = new CassandraSourceConnector();
        var sslEnabledKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.SslEnabledConfig);
        Assert.Equal(false, sslEnabledKey.DefaultValue);
    }

    [Fact]
    public void Config_PasswordIsPasswordType()
    {
        var connector = new CassandraSourceConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_ContactPointsIsHighImportance()
    {
        var connector = new CassandraSourceConnector();
        var contactPointsKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.ContactPointsConfig);
        Assert.Equal(Importance.High, contactPointsKey.Importance);
    }

    [Fact]
    public void Config_KeyspaceIsHighImportance()
    {
        var connector = new CassandraSourceConnector();
        var keyspaceKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.KeyspaceConfig);
        Assert.Equal(Importance.High, keyspaceKey.Importance);
    }
}
