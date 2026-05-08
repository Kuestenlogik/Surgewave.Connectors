using Xunit;
using Kuestenlogik.Surgewave.Connector.Oracle;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Oracle.Tests;

public class OracleCdcSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new OracleCdcSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsOracleCdcSourceTask()
    {
        var connector = new OracleCdcSourceConnector();
        Assert.Equal(typeof(OracleCdcSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new OracleCdcSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.ConnectionString);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.Host);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.Port);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.ServiceName);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.Sid);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.Username);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.Password);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.Tables);
    }

    [Fact]
    public void Config_ContainsOptionalDefinitions()
    {
        var connector = new OracleCdcSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.TopicPrefix);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.TopicPattern);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.SnapshotMode);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.WalletLocation);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.IncludeSchema);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.IncludeBeforeValues);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.LogMinerMode);
        Assert.Contains(connector.Config.Keys, k => k.Name == OracleConnectorConfig.DictionaryMode);
    }

    [Fact]
    public void Start_WithConnectionString_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ConnectionString] = "Data Source=localhost:1521/ORCL;User Id=system;Password=oracle",
            [OracleConnectorConfig.Tables] = "HR.EMPLOYEES,HR.DEPARTMENTS"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithServiceName_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "EMPLOYEES,DEPARTMENTS"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSid_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.Sid] = "XE",
            [OracleConnectorConfig.Tables] = "USERS"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingServiceNameAndConnectionString_ThrowsArgumentException()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.Tables] = "USERS"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingTables_ThrowsArgumentException()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyServiceName_ThrowsArgumentException()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "",
            [OracleConnectorConfig.Tables] = "USERS"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.PollIntervalMs] = "1000"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        // CDC uses single task (LogMiner polling is sequential)
        Assert.Single(taskConfigs);
        Assert.Equal("1000", taskConfigs[0][OracleConnectorConfig.PollIntervalMs]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS"
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
    public void Start_WithSnapshotMode_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.SnapshotMode] = OracleConnectorConfig.SnapshotModeNever
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMultipleTables_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "HR.EMPLOYEES, HR.DEPARTMENTS, SALES.ORDERS"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithFullyQualifiedTables_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "HR.EMPLOYEES, SALES.ORDERS"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Config_HasDefaultHost()
    {
        var connector = new OracleCdcSourceConnector();
        var hostKey = connector.Config.Keys.First(k => k.Name == OracleConnectorConfig.Host);

        Assert.Equal(OracleConnectorConfig.DefaultHost, hostKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultPort()
    {
        var connector = new OracleCdcSourceConnector();
        var portKey = connector.Config.Keys.First(k => k.Name == OracleConnectorConfig.Port);

        Assert.Equal(OracleConnectorConfig.DefaultPort, portKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultTopicPattern()
    {
        var connector = new OracleCdcSourceConnector();
        var patternKey = connector.Config.Keys.First(k => k.Name == OracleConnectorConfig.TopicPattern);

        Assert.Equal(OracleConnectorConfig.DefaultTopicPattern, patternKey.DefaultValue);
    }

    [Fact]
    public void Start_WithUsernameAndPassword_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.Host] = "oracle.example.com",
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Username] = "system",
            [OracleConnectorConfig.Password] = "SecurePassword123!",
            [OracleConnectorConfig.Tables] = "USERS"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithStartFromBeginning_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.StartFromBeginning] = "true"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithLogMinerMode_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.LogMinerMode] = OracleConnectorConfig.LogMinerModeArchived
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithDictionaryMode_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.DictionaryMode] = OracleConnectorConfig.DictionaryModeRedoLog
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWalletLocation_Succeeds()
    {
        var connector = new OracleCdcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [OracleConnectorConfig.ServiceName] = "ORCL",
            [OracleConnectorConfig.Tables] = "USERS",
            [OracleConnectorConfig.WalletLocation] = "/opt/oracle/wallet"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }
}
