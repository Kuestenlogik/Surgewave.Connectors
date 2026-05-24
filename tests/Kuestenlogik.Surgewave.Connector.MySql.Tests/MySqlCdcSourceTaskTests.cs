using Xunit;
using Kuestenlogik.Surgewave.Connector.MySql;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.MySql.Tests;

public class MySqlCdcSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new MySqlCdcSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Host] = "localhost",
            [MySqlConnectorConfig.Port] = "3306",
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Password] = "secret",
            [MySqlConnectorConfig.Tables] = "users,orders"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithDefaultHost_UsesLocalhost()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomServerId_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.ServerId] = "12345"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeNever_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.SnapshotMode] = MySqlConnectorConfig.SnapshotModeNever
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeInitial_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.SnapshotMode] = MySqlConnectorConfig.SnapshotModeInitial
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeAlways_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.SnapshotMode] = MySqlConnectorConfig.SnapshotModeAlways
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeSchemaOnly_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.SnapshotMode] = MySqlConnectorConfig.SnapshotModeSchemaOnly
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSslModePreferred_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.SslMode] = MySqlConnectorConfig.SslModePreferred
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSslModeRequired_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.SslMode] = MySqlConnectorConfig.SslModeRequired
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithTopicPrefix_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.TopicPrefix] = "cdc."
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomTopicPattern_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.TopicPattern] = "${table}"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMultipleTables_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users, orders, products"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBinlogPosition_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.BinlogFilename] = "mysql-bin.000001",
            [MySqlConnectorConfig.BinlogPosition] = "12345"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomPollInterval_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.PollIntervalMs] = "500"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomBatchSize_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.BatchMaxRecords] = "500"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithIncludeSchemaFalse_Succeeds()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users",
            [MySqlConnectorConfig.IncludeSchema] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new MySqlCdcSourceTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users"
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
        var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users"
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
        using var task = new MySqlCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [MySqlConnectorConfig.Database] = "test_db",
            [MySqlConnectorConfig.Username] = "root",
            [MySqlConnectorConfig.Tables] = "users"
        };
        task.Start(config);

        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));

        Assert.Null(exception);
    }
}
