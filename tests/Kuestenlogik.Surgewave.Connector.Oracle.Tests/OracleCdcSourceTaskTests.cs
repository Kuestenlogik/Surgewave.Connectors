using Xunit;
using Kuestenlogik.Surgewave.Connector.Oracle;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Oracle.Tests;

public class OracleCdcSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new OracleCdcSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithConnectionString_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ConnectionString] = "Data Source=localhost:1521/ORCL;User Id=system;Password=oracle",
            [OracleConnectorConfig.Tables] = "HR.EMPLOYEES,HR.DEPARTMENTS"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithServiceName_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.Host] = "localhost",
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSid_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.Host] = "localhost",
            [OracleConnectorConfig.Sid] = "XE",
            [OracleConnectorConfig.Tables] = "USERS"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithDefaultHost_UsesLocalhost()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithUsernameAndPassword_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.Host] = "oracle.example.com",
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Username] = "system",
            [OracleConnectorConfig.Password] = "Password123!",
            [OracleConnectorConfig.Tables] = "USERS"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeNever_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.SnapshotMode] = OracleConnectorConfig.SnapshotModeNever
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeInitial_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.SnapshotMode] = OracleConnectorConfig.SnapshotModeInitial
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeAlways_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.SnapshotMode] = OracleConnectorConfig.SnapshotModeAlways
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSnapshotModeSchemaOnly_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.SnapshotMode] = OracleConnectorConfig.SnapshotModeSchemaOnly
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithTopicPrefix_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.TopicPrefix] = "cdc."
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomTopicPattern_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.TopicPattern] = "${table}"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMultipleTables_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "HR.EMPLOYEES, HR.DEPARTMENTS, SALES.ORDERS"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomPollInterval_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.PollIntervalMs] = "1000"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomBatchSize_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.BatchMaxRecords] = "500"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithIncludeSchemaFalse_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.IncludeSchema] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithIncludeBeforeValuesFalse_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.IncludeBeforeValues] = "false"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartFromBeginning_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.StartFromBeginning] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithLogMinerModeArchived_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.LogMinerMode] = OracleConnectorConfig.LogMinerModeArchived
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithDictionaryModeRedoLog_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.DictionaryMode] = OracleConnectorConfig.DictionaryModeRedoLog
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomPort_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.Host] = "oracle.example.com",
            [OracleConnectorConfig.Port] = "1522",
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWalletLocation_Succeeds()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.WalletLocation] = "/opt/oracle/wallet"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new OracleCdcSourceTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS"
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
        var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS"
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
        using var task = new OracleCdcSourceTask();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS"
        };
        task.Start(config);

        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));

        Assert.Null(exception);
    }
}
