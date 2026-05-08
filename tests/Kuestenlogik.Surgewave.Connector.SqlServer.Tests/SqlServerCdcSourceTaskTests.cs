using Xunit;
using Kuestenlogik.Surgewave.Connector.SqlServer;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SqlServer.Tests;

public class SqlServerCdcSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new SqlServerCdcSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithConnectionString_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.ConnectionString] = "Server=localhost;Database=test_db;Integrated Security=true",
            [SqlServerConnectorConfig.Tables] = "dbo.users,dbo.orders"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithDatabaseName_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Server] = "localhost",
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithDefaultServer_UsesLocalhost()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithUsernameAndPassword_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Server] = "sqlserver.example.com",
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Username] = "sa",
            [SqlServerConnectorConfig.Password] = "Password123!",
            [SqlServerConnectorConfig.Tables] = "users"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeNever_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.SnapshotMode] = SqlServerConnectorConfig.SnapshotModeNever
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeInitial_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.SnapshotMode] = SqlServerConnectorConfig.SnapshotModeInitial
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeAlways_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.SnapshotMode] = SqlServerConnectorConfig.SnapshotModeAlways
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeSchemaOnly_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.SnapshotMode] = SqlServerConnectorConfig.SnapshotModeSchemaOnly
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithTrustServerCertificate_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.TrustServerCertificate] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithEncryptDisabled_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.Encrypt] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithTopicPrefix_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.TopicPrefix] = "cdc."
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomTopicPattern_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.TopicPattern] = "${table}"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMultipleTables_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "dbo.users, dbo.orders, sales.products"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomPollInterval_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.PollIntervalMs] = "1000"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomBatchSize_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.BatchMaxRecords] = "500"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithIncludeSchemaFalse_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.IncludeSchema] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithIncludeBeforeValuesFalse_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.IncludeBeforeValues] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartFromBeginning_Succeeds()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users",
            [SqlServerConnectorConfig.StartFromBeginning] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new SqlServerCdcSourceTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users"
        };
        task.Start(config);

        var exception = Record.Exception(() =>
        {
            task.Stop();
            task.Stop();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users"
        };
        task.Start(config);

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public async Task CommitAsync_CompletesSuccessfully()
    {
        using var task = new SqlServerCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [SqlServerConnectorConfig.Database] = "test_db",
            [SqlServerConnectorConfig.Tables] = "users"
        };
        task.Start(config);

        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));

        Assert.Null(exception);
    }
}
