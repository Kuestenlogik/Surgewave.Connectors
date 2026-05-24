using Kuestenlogik.Surgewave.Connector.Snowflake;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Snowflake.Tests;

public class SnowflakeSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSnowflakeSourceTask()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Equal(typeof(SnowflakeSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesAccountConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.AccountConfig);
    }

    [Fact]
    public void Config_DefinesUserConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.UserConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesDatabaseConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.DatabaseConfig);
    }

    [Fact]
    public void Config_DefinesSchemaConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.SchemaConfig);
    }

    [Fact]
    public void Config_DefinesTableConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.TableConfig);
    }

    [Fact]
    public void Config_DefinesQueryConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.QueryConfig);
    }

    [Fact]
    public void Config_DefinesStreamNameConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.StreamNameConfig);
    }

    [Fact]
    public void Config_DefinesModeConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.ModeConfig);
    }

    [Fact]
    public void Config_DefinesTopicPatternConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesMaxRowsPerPollConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.MaxRowsPerPollConfig);
    }

    [Fact]
    public void Config_DefinesIncludeMetadataConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_DefinesTimestampColumnConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.TimestampColumnConfig);
    }

    [Fact]
    public void Config_DefinesIncrementingColumnConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.IncrementingColumnConfig);
    }

    [Fact]
    public void Config_DefinesWarehouseConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.WarehouseConfig);
    }

    [Fact]
    public void Config_DefinesRoleConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.RoleConfig);
    }

    [Fact]
    public void Config_DefinesAuthenticatorConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.AuthenticatorConfig);
    }

    [Fact]
    public void Config_DefinesPrivateKeyFileConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.PrivateKeyFileConfig);
    }

    [Fact]
    public void Config_DefinesPrivateKeyPassphraseConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.PrivateKeyPassphraseConfig);
    }

    [Fact]
    public void Config_DefinesOAuthTokenConfig()
    {
        var connector = new SnowflakeSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SnowflakeConnectorConfig.OAuthTokenConfig);
    }

    [Fact]
    public void Start_ThrowsWhenAccountMissing()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.AccountConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenUserMissing()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.UserConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenDatabaseMissing()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.TableConfig] = "testtable"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.DatabaseConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTableMissingInTableMode()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.ModeConfig] = "table"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.TableConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenQueryMissingInQueryMode()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.ModeConfig] = "query"
        };

        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.QueryConfig, exception.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTableMissingInStreamMode()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.ModeConfig] = "stream"
        };

        // Table is required for stream mode (stream monitors changes on a table)
        var exception = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SnowflakeConnectorConfig.TableConfig, exception.Message);
    }

    [Fact]
    public void Start_AcceptsValidTableModeConfig()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void Start_AcceptsValidQueryModeConfig()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.ModeConfig] = "query",
            [SnowflakeConnectorConfig.QueryConfig] = "SELECT * FROM testtable"
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void Start_AcceptsValidStreamModeConfig()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.ModeConfig] = "stream",
            [SnowflakeConnectorConfig.TableConfig] = "testtable",  // Table is required (source of the stream)
            [SnowflakeConnectorConfig.StreamNameConfig] = "teststream"  // Optional, auto-created if not provided
        };

        // Should not throw
        connector.Start(config);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleTaskConfig()
    {
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable"
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
        var connector = new SnowflakeSourceConnector();

        var config = new Dictionary<string, string>
        {
            [SnowflakeConnectorConfig.AccountConfig] = "testaccount",
            [SnowflakeConnectorConfig.UserConfig] = "testuser",
            [SnowflakeConnectorConfig.DatabaseConfig] = "testdb",
            [SnowflakeConnectorConfig.TableConfig] = "testtable"
        };

        connector.Start(config);
        connector.Stop();
        // Should complete without exception
    }

    [Fact]
    public void Config_HasExpectedTopicPatternDefault()
    {
        var connector = new SnowflakeSourceConnector();
        var topicPatternKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.TopicPatternConfig);
        Assert.Equal(SnowflakeConnectorConfig.DefaultTopicPattern, topicPatternKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedModeDefault()
    {
        var connector = new SnowflakeSourceConnector();
        var modeKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.ModeConfig);
        Assert.Equal(SnowflakeConnectorConfig.DefaultMode, modeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new SnowflakeSourceConnector();
        var pollIntervalKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.PollIntervalMsConfig);
        Assert.Equal((int)SnowflakeConnectorConfig.DefaultPollIntervalMs, pollIntervalKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRowsPerPollDefault()
    {
        var connector = new SnowflakeSourceConnector();
        var maxRowsKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.MaxRowsPerPollConfig);
        Assert.Equal(SnowflakeConnectorConfig.DefaultMaxRowsPerPoll, maxRowsKey.DefaultValue);
    }

    [Fact]
    public void Config_IncludeMetadataDefaultsToTrue()
    {
        var connector = new SnowflakeSourceConnector();
        var includeMetadataKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.IncludeMetadataConfig);
        Assert.Equal(true, includeMetadataKey.DefaultValue);
    }

    [Fact]
    public void Config_PasswordIsPasswordType()
    {
        var connector = new SnowflakeSourceConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_AccountIsStringType()
    {
        var connector = new SnowflakeSourceConnector();
        var accountKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.AccountConfig);
        Assert.Equal(ConfigType.String, accountKey.Type);
    }

    [Fact]
    public void Config_AccountIsHighImportance()
    {
        var connector = new SnowflakeSourceConnector();
        var accountKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.AccountConfig);
        Assert.Equal(Importance.High, accountKey.Importance);
    }

    [Fact]
    public void Config_DatabaseIsHighImportance()
    {
        var connector = new SnowflakeSourceConnector();
        var databaseKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.DatabaseConfig);
        Assert.Equal(Importance.High, databaseKey.Importance);
    }

    [Fact]
    public void Config_SchemaHasDefaultValue()
    {
        var connector = new SnowflakeSourceConnector();
        var schemaKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.SchemaConfig);
        Assert.Equal("PUBLIC", schemaKey.DefaultValue);
    }

    [Fact]
    public void Config_AuthenticatorHasDefaultValue()
    {
        var connector = new SnowflakeSourceConnector();
        var authKey = connector.Config.Keys.First(k => k.Name == SnowflakeConnectorConfig.AuthenticatorConfig);
        Assert.Equal("snowflake", authKey.DefaultValue);
    }
}
