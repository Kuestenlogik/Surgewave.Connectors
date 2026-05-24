using Xunit;
using Kuestenlogik.Surgewave.Connector.SqlServer;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SqlServer.Tests;

public class SqlServerCdcSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new SqlServerCdcSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSqlServerCdcSourceTask()
    {
        var connector = new SqlServerCdcSourceConnector();
        Assert.Equal(typeof(SqlServerCdcSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new SqlServerCdcSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.ConnectionString);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.Server);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.Database);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.Username);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.Password);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.Tables);
    }

    [Fact]
    public void Config_ContainsOptionalDefinitions()
    {
        var connector = new SqlServerCdcSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.TopicPrefix);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.TopicPattern);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.SnapshotMode);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.TrustServerCertificate);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.Encrypt);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.IncludeSchema);
        Assert.Contains(connector.Config.Keys, k => k.Name == SqlServerConnectorConfig.IncludeBeforeValues);
    }

    [Fact]
    public void Start_WithConnectionString_Succeeds()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.ConnectionString] = "Server=localhost;Database=test_db;Integrated Security=true",
            [SqlServerConnectorConfig.Tables] = "dbo.users,dbo.orders"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithDatabaseName_Succeeds()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users,orders"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingDatabaseAndConnectionString_ThrowsArgumentException()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Tables] = "users"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingTables_ThrowsArgumentException()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyDatabase_ThrowsArgumentException()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "",
            [SqlServerConnectorConfig.Tables] = "users"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.PollIntervalMs] = "1000"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        // CDC uses single task (polling is sequential)
        Assert.Single(taskConfigs);
        Assert.Equal("1000", taskConfigs[0][SqlServerConnectorConfig.PollIntervalMs]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users"
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
    public void Start_WithTrustServerCertificate_Succeeds()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.TrustServerCertificate] = "true"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotMode_Succeeds()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.SnapshotMode] = SqlServerConnectorConfig.SnapshotModeNever
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMultipleTables_Succeeds()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "dbo.users, dbo.orders, sales.products"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithFullyQualifiedTables_Succeeds()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "dbo.users, sales.orders"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Config_HasDefaultServer()
    {
        var connector = new SqlServerCdcSourceConnector();
        var serverKey = connector.Config.Keys.First(k => k.Name == SqlServerConnectorConfig.Server);

        Assert.Equal(SqlServerConnectorConfig.DefaultServer, serverKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultTopicPattern()
    {
        var connector = new SqlServerCdcSourceConnector();
        var patternKey = connector.Config.Keys.First(k => k.Name == SqlServerConnectorConfig.TopicPattern);

        Assert.Equal(SqlServerConnectorConfig.DefaultTopicPattern, patternKey.DefaultValue);
    }

    [Fact]
    public void Start_WithUsernameAndPassword_Succeeds()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Server] = "sqlserver.example.com",
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Username] = "sa",
            [SqlServerConnectorConfig.Password] = "SecurePassword123!",
            [SqlServerConnectorConfig.Tables] = "users"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartFromBeginning_Succeeds()
    {
        var connector = new SqlServerCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.StartFromBeginning] = "true"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }
}
