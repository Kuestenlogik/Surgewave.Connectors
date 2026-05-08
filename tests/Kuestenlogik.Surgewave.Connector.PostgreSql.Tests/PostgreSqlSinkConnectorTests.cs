namespace Kuestenlogik.Surgewave.Connector.PostgreSql.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class PostgreSqlSinkConnectorTests
{
    [Fact]
    public void PostgreSqlSinkConnector_HasCorrectVersion()
    {
        using var connector = new PostgreSqlSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void PostgreSqlSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new PostgreSqlSinkConnector();
        Assert.Equal(typeof(PostgreSqlSinkTask), connector.TaskClass);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new PostgreSqlSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "postgresql.connection" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "topics" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "postgresql.table" && k.Type == ConfigType.String);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new PostgreSqlSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "postgresql.schema");
        Assert.Contains(config.Keys, k => k.Name == "insert.mode");
        Assert.Contains(config.Keys, k => k.Name == "pk.mode");
        Assert.Contains(config.Keys, k => k.Name == "pk.fields");
        Assert.Contains(config.Keys, k => k.Name == "batch.size");
        Assert.Contains(config.Keys, k => k.Name == "retry.max");
        Assert.Contains(config.Keys, k => k.Name == "retry.backoff.ms");
    }

    [Fact]
    public void PostgreSqlSinkConnector_Start_ThrowsOnMissingConnection()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topics"] = "test-topic",
            ["postgresql.table"] = "users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("postgresql.connection", ex.Message);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["postgresql.table"] = "users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Start_ThrowsOnMissingTable()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("postgresql.table", ex.Message);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Start_AcceptsValidConfig()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "users"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("insert")]
    [InlineData("upsert")]
    public void PostgreSqlSinkConnector_Start_AcceptsValidInsertModes(string mode)
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "users",
            ["insert.mode"] = mode,
            ["pk.fields"] = mode == "upsert" ? "id" : ""
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void PostgreSqlSinkConnector_Start_ThrowsOnInvalidInsertMode()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "users",
            ["insert.mode"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid insert mode", ex.Message);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Start_ThrowsOnUpsertWithoutPkFields()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "users",
            ["insert.mode"] = "upsert"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("pk.fields", ex.Message);
    }

    [Theory]
    [InlineData("record_key")]
    [InlineData("record_value")]
    public void PostgreSqlSinkConnector_Start_AcceptsValidPkModes(string mode)
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "users",
            ["pk.mode"] = mode
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void PostgreSqlSinkConnector_Start_ThrowsOnInvalidPkMode()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "users",
            ["pk.mode"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid pk.mode", ex.Message);
    }

    [Fact]
    public void PostgreSqlSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "users"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("users", taskConfigs[0]["postgresql.table"]);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new PostgreSqlSinkConnector();
        var config = connector.Config;

        var schemaKey = config.Keys.First(k => k.Name == "postgresql.schema");
        Assert.Equal("public", schemaKey.DefaultValue);

        var insertModeKey = config.Keys.First(k => k.Name == "insert.mode");
        Assert.Equal("insert", insertModeKey.DefaultValue);

        var pkModeKey = config.Keys.First(k => k.Name == "pk.mode");
        Assert.Equal("record_key", pkModeKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == "batch.size");
        Assert.Equal(100L, batchSizeKey.DefaultValue);

        var retryMaxKey = config.Keys.First(k => k.Name == "retry.max");
        Assert.Equal(3L, retryMaxKey.DefaultValue);

        var retryBackoffKey = config.Keys.First(k => k.Name == "retry.backoff.ms");
        Assert.Equal(1000L, retryBackoffKey.DefaultValue);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Start_AcceptsUpsertWithPkFields()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "users",
            ["insert.mode"] = "upsert",
            ["pk.fields"] = "id,tenant_id"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void PostgreSqlSinkConnector_Config_HasPgvectorKeys()
    {
        using var connector = new PostgreSqlSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == PostgreSqlConnectorConfig.VectorFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == PostgreSqlConnectorConfig.VectorDimensionsConfig);
        Assert.Contains(config.Keys, k => k.Name == PostgreSqlConnectorConfig.VectorCreateExtensionConfig);
        Assert.Contains(config.Keys, k => k.Name == PostgreSqlConnectorConfig.VectorIndexTypeConfig);
        Assert.Contains(config.Keys, k => k.Name == PostgreSqlConnectorConfig.VectorDistanceMetricConfig);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Config_HasCorrectPgvectorDefaultValues()
    {
        using var connector = new PostgreSqlSinkConnector();
        var config = connector.Config;

        var vectorDimensionsKey = config.Keys.First(k => k.Name == PostgreSqlConnectorConfig.VectorDimensionsConfig);
        Assert.Equal((long)PostgreSqlConnectorConfig.DefaultVectorDimensions, vectorDimensionsKey.DefaultValue);

        var createExtensionKey = config.Keys.First(k => k.Name == PostgreSqlConnectorConfig.VectorCreateExtensionConfig);
        Assert.Equal(true, createExtensionKey.DefaultValue);

        var indexTypeKey = config.Keys.First(k => k.Name == PostgreSqlConnectorConfig.VectorIndexTypeConfig);
        Assert.Equal(PostgreSqlConnectorConfig.VectorIndexNone, indexTypeKey.DefaultValue);

        var distanceMetricKey = config.Keys.First(k => k.Name == PostgreSqlConnectorConfig.VectorDistanceMetricConfig);
        Assert.Equal(PostgreSqlConnectorConfig.VectorDistanceCosine, distanceMetricKey.DefaultValue);
    }

    [Fact]
    public void PostgreSqlSinkConnector_Start_AcceptsVectorConfig()
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "embeddings",
            [PostgreSqlConnectorConfig.VectorFieldConfig] = "embedding",
            [PostgreSqlConnectorConfig.VectorDimensionsConfig] = "1536",
            [PostgreSqlConnectorConfig.VectorCreateExtensionConfig] = "true",
            [PostgreSqlConnectorConfig.VectorIndexTypeConfig] = PostgreSqlConnectorConfig.VectorIndexHnsw,
            [PostgreSqlConnectorConfig.VectorDistanceMetricConfig] = PostgreSqlConnectorConfig.VectorDistanceCosine
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData(PostgreSqlConnectorConfig.VectorIndexNone)]
    [InlineData(PostgreSqlConnectorConfig.VectorIndexIvfflat)]
    [InlineData(PostgreSqlConnectorConfig.VectorIndexHnsw)]
    public void PostgreSqlSinkConnector_Start_AcceptsValidVectorIndexTypes(string indexType)
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "embeddings",
            [PostgreSqlConnectorConfig.VectorFieldConfig] = "embedding",
            [PostgreSqlConnectorConfig.VectorIndexTypeConfig] = indexType
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData(PostgreSqlConnectorConfig.VectorDistanceCosine)]
    [InlineData(PostgreSqlConnectorConfig.VectorDistanceL2)]
    [InlineData(PostgreSqlConnectorConfig.VectorDistanceInnerProduct)]
    public void PostgreSqlSinkConnector_Start_AcceptsValidVectorDistanceMetrics(string metric)
    {
        using var connector = new PostgreSqlSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["postgresql.connection"] = "Host=localhost;Database=test;Username=test;Password=test",
            ["topics"] = "test-topic",
            ["postgresql.table"] = "embeddings",
            [PostgreSqlConnectorConfig.VectorFieldConfig] = "embedding",
            [PostgreSqlConnectorConfig.VectorDistanceMetricConfig] = metric
        };

        // Should not throw
        connector.Start(config);
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
