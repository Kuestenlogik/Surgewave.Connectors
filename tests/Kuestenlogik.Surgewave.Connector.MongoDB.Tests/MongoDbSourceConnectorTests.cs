namespace Kuestenlogik.Surgewave.Connector.MongoDB.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class MongoDbSourceConnectorTests
{
    [Fact]
    public void MongoDbSourceConnector_HasCorrectVersion()
    {
        using var connector = new MongoDbSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void MongoDbSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new MongoDbSourceConnector();
        Assert.Equal(typeof(MongoDbSourceTask), connector.TaskClass);
    }

    [Fact]
    public void MongoDbSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new MongoDbSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "mongodb.connection" && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == "mongodb.database" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "mongodb.collection" && k.Type == ConfigType.String);
    }

    [Fact]
    public void MongoDbSourceConnector_Config_HasOptionalKeys()
    {
        using var connector = new MongoDbSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "source.mode");
        Assert.Contains(config.Keys, k => k.Name == "topic.prefix");
        Assert.Contains(config.Keys, k => k.Name == "topic.pattern");
        Assert.Contains(config.Keys, k => k.Name == "change.stream.full.document");
        Assert.Contains(config.Keys, k => k.Name == "poll.field");
        Assert.Contains(config.Keys, k => k.Name == "poll.interval.ms");
        Assert.Contains(config.Keys, k => k.Name == "batch.max.records");
        Assert.Contains(config.Keys, k => k.Name == "pipeline");
    }

    [Fact]
    public void MongoDbSourceConnector_Start_ThrowsOnMissingConnection()
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mongodb.connection", ex.Message);
    }

    [Fact]
    public void MongoDbSourceConnector_Start_ThrowsOnMissingDatabase()
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.collection"] = "users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mongodb.database", ex.Message);
    }

    [Fact]
    public void MongoDbSourceConnector_Start_ThrowsOnMissingCollection()
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mongodb.collection", ex.Message);
    }

    [Fact]
    public void MongoDbSourceConnector_Start_AcceptsValidConfig()
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("change_stream")]
    [InlineData("poll")]
    public void MongoDbSourceConnector_Start_AcceptsValidSourceModes(string mode)
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["source.mode"] = mode
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void MongoDbSourceConnector_Start_ThrowsOnInvalidSourceMode()
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["source.mode"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid source mode", ex.Message);
    }

    [Theory]
    [InlineData("default")]
    [InlineData("updateLookup")]
    [InlineData("whenAvailable")]
    public void MongoDbSourceConnector_Start_AcceptsValidFullDocumentModes(string mode)
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["change.stream.full.document"] = mode
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void MongoDbSourceConnector_Start_ThrowsOnInvalidFullDocumentMode()
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users",
            ["change.stream.full.document"] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid full document mode", ex.Message);
    }

    [Fact]
    public void MongoDbSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "users"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        // Change streams and polling both use a single cursor
        Assert.Single(taskConfigs);
        Assert.Equal("users", taskConfigs[0]["mongodb.collection"]);
    }

    [Fact]
    public void MongoDbSourceConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new MongoDbSourceConnector();
        var config = connector.Config;

        var sourceModeKey = config.Keys.First(k => k.Name == "source.mode");
        Assert.Equal("change_stream", sourceModeKey.DefaultValue);

        var topicPatternKey = config.Keys.First(k => k.Name == "topic.pattern");
        Assert.Equal("${database}.${collection}", topicPatternKey.DefaultValue);

        var fullDocumentKey = config.Keys.First(k => k.Name == "change.stream.full.document");
        Assert.Equal("updateLookup", fullDocumentKey.DefaultValue);

        var pollFieldKey = config.Keys.First(k => k.Name == "poll.field");
        Assert.Equal("_id", pollFieldKey.DefaultValue);

        var pollIntervalKey = config.Keys.First(k => k.Name == "poll.interval.ms");
        Assert.Equal(1000L, pollIntervalKey.DefaultValue);

        var batchMaxRecordsKey = config.Keys.First(k => k.Name == "batch.max.records");
        Assert.Equal(500L, batchMaxRecordsKey.DefaultValue);
    }

    [Fact]
    public void MongoDbSourceConnector_Start_AcceptsWildcardCollection()
    {
        using var connector = new MongoDbSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mongodb.connection"] = "mongodb://localhost:27017",
            ["mongodb.database"] = "mydb",
            ["mongodb.collection"] = "*"
        };

        // Should not throw - * means watch all collections
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
