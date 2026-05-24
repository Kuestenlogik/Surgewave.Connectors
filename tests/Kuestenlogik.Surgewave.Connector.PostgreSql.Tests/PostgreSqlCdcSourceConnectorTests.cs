namespace Kuestenlogik.Surgewave.Connector.PostgreSql.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class PostgreSqlCdcSourceConnectorTests
{
    [Fact]
    public void PostgreSqlCdcSourceConnector_HasCorrectVersion()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        Assert.Equal(typeof(PostgreSqlCdcSourceTask), connector.TaskClass);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "postgresql.connection" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "postgresql.tables" && k.Type == ConfigType.String);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Config_HasOptionalKeys()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "postgresql.slot.name");
        Assert.Contains(config.Keys, k => k.Name == "postgresql.publication.name");
        Assert.Contains(config.Keys, k => k.Name == "postgresql.create.slot");
        Assert.Contains(config.Keys, k => k.Name == "postgresql.create.publication");
        Assert.Contains(config.Keys, k => k.Name == "topic.prefix");
        Assert.Contains(config.Keys, k => k.Name == "topic.pattern");
        Assert.Contains(config.Keys, k => k.Name == "include.schema");
        Assert.Contains(config.Keys, k => k.Name == "include.before.values");
        Assert.Contains(config.Keys, k => k.Name == "snapshot.mode");
        Assert.Contains(config.Keys, k => k.Name == "poll.interval.ms");
        Assert.Contains(config.Keys, k => k.Name == "batch.max.records");
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Start_ThrowsOnMissingConnection()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.tables"] = "public.users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("postgresql.connection", ex.Message);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Start_ThrowsOnMissingTables()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("postgresql.tables", ex.Message);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Start_AcceptsValidConfig()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["postgresql.tables"] = "public.users,public.orders"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("initial")]
    [InlineData("never")]
    [InlineData("always")]
    public void PostgreSqlCdcSourceConnector_Start_AcceptsValidSnapshotModes(string mode)
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["postgresql.tables"] = "public.users",
            ["snapshot.mode"] = mode
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Start_ThrowsOnInvalidSnapshotMode()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["postgresql.tables"] = "public.users",
            ["snapshot.mode"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid snapshot mode", ex.Message);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["postgresql.tables"] = "public.users,public.orders"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        // CDC replication slot can only be consumed by a single consumer
        Assert.Single(taskConfigs);
        Assert.Equal("public.users,public.orders", taskConfigs[0]["postgresql.tables"]);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        var config = connector.Config;

        var slotNameKey = config.Keys.First(k => k.Name == "postgresql.slot.name");
        Assert.Equal("surgewave_cdc_slot", slotNameKey.DefaultValue);

        var publicationKey = config.Keys.First(k => k.Name == "postgresql.publication.name");
        Assert.Equal("surgewave_publication", publicationKey.DefaultValue);

        var topicPatternKey = config.Keys.First(k => k.Name == "topic.pattern");
        Assert.Equal("${schema}.${table}", topicPatternKey.DefaultValue);

        var snapshotModeKey = config.Keys.First(k => k.Name == "snapshot.mode");
        Assert.Equal("initial", snapshotModeKey.DefaultValue);

        var pollIntervalKey = config.Keys.First(k => k.Name == "poll.interval.ms");
        Assert.Equal(100L, pollIntervalKey.DefaultValue);

        var batchMaxRecordsKey = config.Keys.First(k => k.Name == "batch.max.records");
        Assert.Equal(500L, batchMaxRecordsKey.DefaultValue);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Config_CreateSlotDefaultsToTrue()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        var config = connector.Config;

        var createSlotKey = config.Keys.First(k => k.Name == "postgresql.create.slot");
        Assert.Equal(true, createSlotKey.DefaultValue);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Config_CreatePublicationDefaultsToTrue()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        var config = connector.Config;

        var createPublicationKey = config.Keys.First(k => k.Name == "postgresql.create.publication");
        Assert.Equal(true, createPublicationKey.DefaultValue);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Config_IncludeSchemaDefaultsToTrue()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        var config = connector.Config;

        var includeSchemaKey = config.Keys.First(k => k.Name == "include.schema");
        Assert.Equal(true, includeSchemaKey.DefaultValue);
    }

    [Fact]
    public void PostgreSqlCdcSourceConnector_Config_IncludeBeforeValuesDefaultsToTrue()
    {
        using var connector = new PostgreSqlCdcSourceConnector();
        var config = connector.Config;

        var includeBeforeValuesKey = config.Keys.First(k => k.Name == "include.before.values");
        Assert.Equal(true, includeBeforeValuesKey.DefaultValue);
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
