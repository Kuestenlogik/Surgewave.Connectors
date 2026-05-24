namespace Kuestenlogik.Surgewave.Connector.MongoDB.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class MongoDbSinkConnectorTests
{
    [Fact]
    public void MongoDbSinkConnector_HasCorrectVersion()
    {
        using var connector = new MongoDbSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void MongoDbSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new MongoDbSinkConnector();
        Assert.Equal(typeof(MongoDbSinkTask), connector.TaskClass);
    }

    [Fact]
    public void MongoDbSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new MongoDbSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "mongodb.connection" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "mongodb.database" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "mongodb.collection" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "topics" && k.Type == ConfigType.String);
    }

    [Fact]
    public void MongoDbSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new MongoDbSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "write.mode");
        Assert.Contains(config.Keys, k => k.Name == "document.id.strategy");
        Assert.Contains(config.Keys, k => k.Name == "document.id.field");
        Assert.Contains(config.Keys, k => k.Name == "batch.size");
        Assert.Contains(config.Keys, k => k.Name == "write.concern");
        Assert.Contains(config.Keys, k => k.Name == "retry.max");
        Assert.Contains(config.Keys, k => k.Name == "retry.backoff.ms");
    }

    [Fact]
    public void MongoDbSinkConnector_Start_ThrowsOnMissingConnection()
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mongodb.connection", ex.Message);
    }

    [Fact]
    public void MongoDbSinkConnector_Start_ThrowsOnMissingDatabase()
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mongodb.database", ex.Message);
    }

    [Fact]
    public void MongoDbSinkConnector_Start_ThrowsOnMissingCollection()
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["topics"] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mongodb.collection", ex.Message);
    }

    [Fact]
    public void MongoDbSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void MongoDbSinkConnector_Start_AcceptsValidConfig()
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("insert")]
    [InlineData("upsert")]
    [InlineData("replace")]
    public void MongoDbSinkConnector_Start_AcceptsValidWriteModes(string mode)
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic",
            ["write.mode"] = mode
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void MongoDbSinkConnector_Start_ThrowsOnInvalidWriteMode()
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic",
            ["write.mode"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid write mode", ex.Message);
    }

    [Theory]
    [InlineData("auto")]
    [InlineData("key")]
    [InlineData("field")]
    public void MongoDbSinkConnector_Start_AcceptsValidDocIdStrategies(string strategy)
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic",
            ["document.id.strategy"] = strategy
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void MongoDbSinkConnector_Start_ThrowsOnInvalidDocIdStrategy()
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic",
            ["document.id.strategy"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid document ID strategy", ex.Message);
    }

    [Theory]
    [InlineData("w1")]
    [InlineData("majority")]
    [InlineData("unacknowledged")]
    public void MongoDbSinkConnector_Start_AcceptsValidWriteConcerns(string concern)
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic",
            ["write.concern"] = concern
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void MongoDbSinkConnector_Start_ThrowsOnInvalidWriteConcern()
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic",
            ["write.concern"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid write concern", ex.Message);
    }

    [Fact]
    public void MongoDbSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new MongoDbSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["topics"] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("users", taskConfigs[0]["mongodb.collection"]);
    }

    [Fact]
    public void MongoDbSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new MongoDbSinkConnector();
        var config = connector.Config;

        var writeModeKey = config.Keys.First(k => k.Name == "write.mode");
        Assert.Equal("insert", writeModeKey.DefaultValue);

        var docIdStrategyKey = config.Keys.First(k => k.Name == "document.id.strategy");
        Assert.Equal("auto", docIdStrategyKey.DefaultValue);

        var docIdFieldKey = config.Keys.First(k => k.Name == "document.id.field");
        Assert.Equal("_id", docIdFieldKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == "batch.size");
        Assert.Equal(100L, batchSizeKey.DefaultValue);

        var writeConcernKey = config.Keys.First(k => k.Name == "write.concern");
        Assert.Equal("majority", writeConcernKey.DefaultValue);

        var retryMaxKey = config.Keys.First(k => k.Name == "retry.max");
        Assert.Equal(3L, retryMaxKey.DefaultValue);

        var retryBackoffKey = config.Keys.First(k => k.Name == "retry.backoff.ms");
        Assert.Equal(1000L, retryBackoffKey.DefaultValue);
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
