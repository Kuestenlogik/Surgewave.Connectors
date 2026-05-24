using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Database;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Database.Tests;

public sealed class DatabaseConnectorTests
{
    [Fact]
    public void DatabaseSourceConnector_HasCorrectConfig()
    {
        using var connector = new DatabaseSourceConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(DatabaseSourceTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == "connection.string");
        Assert.Contains(configKeys, k => k.Name == "db.provider");
        Assert.Contains(configKeys, k => k.Name == "mode");
        Assert.Contains(configKeys, k => k.Name == "table.whitelist");
    }

    [Fact]
    public void DatabaseSinkConnector_HasCorrectConfig()
    {
        using var connector = new DatabaseSinkConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(DatabaseSinkTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == "connection.string");
        Assert.Contains(configKeys, k => k.Name == "topics");
        Assert.Contains(configKeys, k => k.Name == "insert.mode");
        Assert.Contains(configKeys, k => k.Name == "auto.create");
    }

    [Fact]
    public void DatabaseSourceConnector_ThrowsOnMissingConnectionString()
    {
        using var connector = new DatabaseSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["table.whitelist"] = "users,orders"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void DatabaseSinkConnector_ThrowsOnMissingConfig()
    {
        using var connector = new DatabaseSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["connection.string"] = "Server=localhost;Database=test"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void DatabaseSourceConnector_ProducesTaskConfigs()
    {
        using var connector = new DatabaseSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["connection.string"] = "Server=localhost;Database=test",
            ["table.whitelist"] = "users",
            ["mode"] = "incrementing"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("Server=localhost;Database=test", taskConfigs[0]["connection.string"]);
        Assert.Equal("users", taskConfigs[0]["table.whitelist"]);

        connector.Stop();
    }

    private static ConnectorContext CreateContext()
    {
        return new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { },
            Logger = null
        };
    }
}
