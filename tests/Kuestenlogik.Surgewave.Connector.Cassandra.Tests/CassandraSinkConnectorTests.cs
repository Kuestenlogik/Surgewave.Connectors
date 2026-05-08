using Kuestenlogik.Surgewave.Connector.Cassandra;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Cassandra.Tests;

public class CassandraSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new CassandraSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsCassandraSinkTask()
    {
        var connector = new CassandraSinkConnector();
        Assert.Equal(typeof(CassandraSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesContactPointsConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.ContactPointsConfig);
    }

    [Fact]
    public void Config_DefinesPortConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.PortConfig);
    }

    [Fact]
    public void Config_DefinesDatacenterConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.DatacenterConfig);
    }

    [Fact]
    public void Config_DefinesKeyspaceConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.KeyspaceConfig);
    }

    [Fact]
    public void Config_DefinesTableConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.TableConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesConsistencyLevelConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.ConsistencyLevelConfig);
    }

    [Fact]
    public void Config_DefinesSslEnabledConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.SslEnabledConfig);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesWriteModeConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.WriteModeConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesMaxRetryCountConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void Config_DefinesRetryDelayMsConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Config_DefinesBatchTypeConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.BatchTypeConfig);
    }

    [Fact]
    public void Config_DefinesTtlSecondsConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.TtlSecondsConfig);
    }

    [Fact]
    public void Config_DefinesPartitionKeyColumnsConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.PartitionKeyColumnsConfig);
    }

    [Fact]
    public void Config_DefinesClusteringKeyColumnsConfig()
    {
        var connector = new CassandraSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == CassandraConnectorConfig.ClusteringKeyColumnsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenContactPointsMissing()
    {
        var connector = new CassandraSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TableConfig] = "testtable",
            [CassandraConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CassandraConnectorConfig.ContactPointsConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenKeyspaceMissing()
    {
        var connector = new CassandraSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.TableConfig] = "testtable",
            [CassandraConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CassandraConnectorConfig.KeyspaceConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTableMissing()
    {
        var connector = new CassandraSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CassandraConnectorConfig.TableConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new CassandraSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(CassandraConnectorConfig.TopicsConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsValidConfig()
    {
        var connector = new CassandraSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TableConfig] = "testtable",
            [CassandraConnectorConfig.TopicsConfig] = "testtopic"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new CassandraSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TableConfig] = "testtable",
            [CassandraConnectorConfig.TopicsConfig] = "testtopic"
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
        var connector = new CassandraSinkConnector();

        var config = new Dictionary<string, string>
        {
            [CassandraConnectorConfig.ContactPointsConfig] = "localhost",
            [CassandraConnectorConfig.KeyspaceConfig] = "testkeyspace",
            [CassandraConnectorConfig.TableConfig] = "testtable",
            [CassandraConnectorConfig.TopicsConfig] = "testtopic"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedWriteModeDefault()
    {
        var connector = new CassandraSinkConnector();
        var writeModeKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.WriteModeConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultWriteMode, writeModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedBatchSizeDefault()
    {
        var connector = new CassandraSinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.BatchSizeConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRetryCountDefault()
    {
        var connector = new CassandraSinkConnector();
        var maxRetryKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.MaxRetryCountConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultMaxRetryCount, maxRetryKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedRetryDelayDefault()
    {
        var connector = new CassandraSinkConnector();
        var retryDelayKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.RetryDelayMsConfig);
        Assert.Equal((int)CassandraConnectorConfig.DefaultRetryDelayMs, retryDelayKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedBatchTypeDefault()
    {
        var connector = new CassandraSinkConnector();
        var batchTypeKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.BatchTypeConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultBatchType, batchTypeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedTtlSecondsDefault()
    {
        var connector = new CassandraSinkConnector();
        var ttlKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.TtlSecondsConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultTtlSeconds, ttlKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPortDefault()
    {
        var connector = new CassandraSinkConnector();
        var portKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.PortConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultPort, portKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedConsistencyDefault()
    {
        var connector = new CassandraSinkConnector();
        var consistencyKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.ConsistencyLevelConfig);
        Assert.Equal(CassandraConnectorConfig.DefaultConsistencyLevel, consistencyKey.DefaultValue);
    }

    [Fact]
    public void Config_PasswordIsPasswordType()
    {
        var connector = new CassandraSinkConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_ContactPointsIsHighImportance()
    {
        var connector = new CassandraSinkConnector();
        var contactPointsKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.ContactPointsConfig);
        Assert.Equal(Importance.High, contactPointsKey.Importance);
    }

    [Fact]
    public void Config_KeyspaceIsHighImportance()
    {
        var connector = new CassandraSinkConnector();
        var keyspaceKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.KeyspaceConfig);
        Assert.Equal(Importance.High, keyspaceKey.Importance);
    }

    [Fact]
    public void Config_TableIsHighImportance()
    {
        var connector = new CassandraSinkConnector();
        var tableKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.TableConfig);
        Assert.Equal(Importance.High, tableKey.Importance);
    }

    [Fact]
    public void Config_TopicsIsHighImportance()
    {
        var connector = new CassandraSinkConnector();
        var topicsKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, topicsKey.Importance);
    }

    [Fact]
    public void Config_SslEnabledDefaultsToFalse()
    {
        var connector = new CassandraSinkConnector();
        var sslEnabledKey = connector.Config.Keys.First(k => k.Name == CassandraConnectorConfig.SslEnabledConfig);
        Assert.Equal(false, sslEnabledKey.DefaultValue);
    }
}
