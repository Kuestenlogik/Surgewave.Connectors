using Xunit;
using Kuestenlogik.Surgewave.Connector.MySql;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.MySql.Tests;

public class MySqlCdcSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new MySqlCdcSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsMySqlCdcSourceTask()
    {
        var connector = new MySqlCdcSourceConnector();
        Assert.Equal(typeof(MySqlCdcSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new MySqlCdcSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.Host);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.Port);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.Database);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.Username);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.Password);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.Tables);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.ServerId);
    }

    [Fact]
    public void Config_ContainsOptionalDefinitions()
    {
        var connector = new MySqlCdcSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.TopicPrefix);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.TopicPattern);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.SnapshotMode);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.SslMode);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.IncludeSchema);
        Assert.Contains(connector.Config.Keys, k => k.Name == MySqlConnectorConfig.IncludeBeforeValues);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users,orders"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingDatabase_ThrowsArgumentException()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingUsername_ThrowsArgumentException()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Tables] = "users"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingTables_ThrowsArgumentException()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyDatabase_ThrowsArgumentException()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.ServerId] = "12345"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        // CDC uses single task (binlog is sequential)
        Assert.Single(taskConfigs);
        Assert.Equal("12345", taskConfigs[0][MySqlConnectorConfig.ServerId]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users"
        };
        connector.Start(config);

        var exception = Record.Exception(() =>
        {
            connector.Stop();
            connector.Stop();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSslMode_Succeeds()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.SslMode] = MySqlConnectorConfig.SslModeRequired
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotMode_Succeeds()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.SnapshotMode] = MySqlConnectorConfig.SnapshotModeNever
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMultipleTables_Succeeds()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users, orders, products"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithFullyQualifiedTables_Succeeds()
    {
        var connector = new MySqlCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "mydb.users, otherdb.orders"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Config_HasDefaultHost()
    {
        var connector = new MySqlCdcSourceConnector();
        var hostKey = connector.Config.Keys.First(k => k.Name == MySqlConnectorConfig.Host);

        Assert.Equal(MySqlConnectorConfig.DefaultHost, hostKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultPort()
    {
        var connector = new MySqlCdcSourceConnector();
        var portKey = connector.Config.Keys.First(k => k.Name == MySqlConnectorConfig.Port);

        Assert.Equal(MySqlConnectorConfig.DefaultPort, portKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultTopicPattern()
    {
        var connector = new MySqlCdcSourceConnector();
        var patternKey = connector.Config.Keys.First(k => k.Name == MySqlConnectorConfig.TopicPattern);

        Assert.Equal(MySqlConnectorConfig.DefaultTopicPattern, patternKey.DefaultValue);
    }
}
