using Kuestenlogik.Surgewave.Connector.Snowflake;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Snowflake.Tests;

public class SnowflakeSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSnowflakeSinkTask()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Equal(typeof(SnowflakeSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesAccountConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.AccountConfig);
    }

    [Fact]
    public void Config_DefinesUserConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.UserConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesDatabaseConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.DatabaseConfig);
    }

    [Fact]
    public void Config_DefinesSchemaConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.SchemaConfig);
    }

    [Fact]
    public void Config_DefinesTableConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.TableConfig);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesWriteModeConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.WriteModeConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesKeyColumnsConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.KeyColumnsConfig);
    }

    [Fact]
    public void Config_DefinesMaxRetryCountConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void Config_DefinesRetryDelayMsConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Config_DefinesAutoCreateTableConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.AutoCreateTableConfig);
    }

    [Fact]
    public void Config_DefinesStageNameConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.StageNameConfig);
    }

    [Fact]
    public void Config_DefinesUseSnowpipeConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.UseSnowpipeConfig);
    }

    [Fact]
    public void Config_DefinesPipeNameConfig()
    {
        var connector = new SnowflakeSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.PipeNameConfig);
    }

    [Fact]
    public void Start_ThrowsWhenAccountMissing()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.AccountConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenUserMissing()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.UserConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenDatabaseMissing()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.DatabaseConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTableMissing()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.TableConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.TopicsConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenKeyColumnsMissingForUpsertMode()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic",
            [SnowflakeConnectorConfig.WriteModeConfig] = "upsert"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.KeyColumnsConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenKeyColumnsMissingForMergeMode()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic",
            [SnowflakeConnectorConfig.WriteModeConfig] = "merge"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.KeyColumnsConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsValidInsertModeConfig()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void Start_AcceptsValidUpsertModeWithKeyColumns()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic",
            [SnowflakeConnectorConfig.WriteModeConfig] = "upsert",
            [SnowflakeConnectorConfig.KeyColumnsConfig] = "id"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void Start_AcceptsValidMergeModeWithKeyColumns()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic",
            [SnowflakeConnectorConfig.WriteModeConfig] = "merge",
            [SnowflakeConnectorConfig.KeyColumnsConfig] = "id,tenant_id"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        Assert.Single(taskConfigs);
        Assert.Equal("testaccount", taskConfigs[0][SnowflakeConnectorConfig.AccountConfig]);
        Assert.Equal("testuser", taskConfigs[0][SnowflakeConnectorConfig.UserConfig]);
        Assert.Equal("testdb", taskConfigs[0][SnowflakeConnectorConfig.DatabaseConfig]);
        Assert.Equal("testtable", taskConfigs[0][SnowflakeConnectorConfig.TableConfig]);
    }

    [Fact]
    public void Stop_CompletesWithoutError()
    {
        var connector = new SnowflakeSinkConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",
            [SnowflakeConnectorConfig.TopicsConfig] = "testtopic"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedWriteModeDefault()
    {
        var connector = new SnowflakeSinkConnector();
        var writeModeKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.WriteModeConfig);
        Assert.Equal(SnowflakeConnectorConfig.DefaultWriteMode, writeModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedBatchSizeDefault()
    {
        var connector = new SnowflakeSinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.BatchSizeConfig);
        Assert.Equal(SnowflakeConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRetryCountDefault()
    {
        var connector = new SnowflakeSinkConnector();
        var maxRetryKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.MaxRetryCountConfig);
        Assert.Equal(SnowflakeConnectorConfig.DefaultMaxRetryCount, maxRetryKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedRetryDelayDefault()
    {
        var connector = new SnowflakeSinkConnector();
        var retryDelayKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.RetryDelayMsConfig);
        Assert.Equal((int)SnowflakeConnectorConfig.DefaultRetryDelayMs, retryDelayKey.DefaultValue);
    }

    [Fact]
    public void Config_PasswordIsPasswordType()
    {
        var connector = new SnowflakeSinkConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_AccountIsHighImportance()
    {
        var connector = new SnowflakeSinkConnector();
        var accountKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.AccountConfig);
        Assert.Equal(Importance.High, accountKey.Importance);
    }

    [Fact]
    public void Config_TopicsIsHighImportance()
    {
        var connector = new SnowflakeSinkConnector();
        var topicsKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, topicsKey.Importance);
    }

    [Fact]
    public void Config_AutoCreateTableDefaultsToFalse()
    {
        var connector = new SnowflakeSinkConnector();
        var autoCreateKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.AutoCreateTableConfig);
        Assert.Equal(false, autoCreateKey.DefaultValue);
    }

    [Fact]
    public void Config_UseSnowpipeDefaultsToFalse()
    {
        var connector = new SnowflakeSinkConnector();
        var snowpipeKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.UseSnowpipeConfig);
        Assert.Equal(false, snowpipeKey.DefaultValue);
    }

    [Fact]
    public void Config_SchemaHasDefaultValue()
    {
        var connector = new SnowflakeSinkConnector();
        var schemaKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.SchemaConfig);
        Assert.Equal("PUBLIC", schemaKey.DefaultValue);
    }
}
