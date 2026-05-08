using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Neo4j;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Neo4j.Tests;

public class Neo4jSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsNeo4jSourceTask()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Equal(typeof(Neo4jSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesUriConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.UriConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesDatabaseConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.DatabaseConfig);
    }

    [Fact]
    public void Config_DefinesLabelConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.LabelConfig);
    }

    [Fact]
    public void Config_DefinesQueryConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.QueryConfig);
    }

    [Fact]
    public void Config_DefinesTopicPatternConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesMaxRowsPerPollConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.MaxRowsPerPollConfig);
    }

    [Fact]
    public void Config_DefinesIncludeMetadataConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_DefinesTimestampPropertyConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.TimestampPropertyConfig);
    }

    [Fact]
    public void Config_DefinesIdPropertyConfig()
    {
        var connector = new Neo4jSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.IdPropertyConfig);
    }

    [Fact]
    public void Start_ThrowsWhenUriMissing()
    {
        var connector = new Neo4jSourceConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.LabelConfig] = "Person"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(Neo4jConnectorConfig.UriConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenLabelAndQueryBothMissing()
    {
        var connector = new Neo4jSourceConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(Neo4jConnectorConfig.LabelConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithLabel()
    {
        var connector = new Neo4jSourceConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.LabelConfig] = "Person"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_SucceedsWithCustomQuery()
    {
        var connector = new Neo4jSourceConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.QueryConfig] = "MATCH (n:Person) RETURN n"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new Neo4jSourceConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.LabelConfig] = "Person"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[Neo4jConnectorConfig.UriConfig], taskConfigs[0][Neo4jConnectorConfig.UriConfig]);
        Assert.Equal(config[Neo4jConnectorConfig.LabelConfig], taskConfigs[0][Neo4jConnectorConfig.LabelConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new Neo4jSourceConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.LabelConfig] = "Person"
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
    public void Config_PasswordIsPasswordType()
    {
        var connector = new Neo4jSourceConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_UriIsHighImportance()
    {
        var connector = new Neo4jSourceConnector();
        var uriKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.UriConfig);
        Assert.Equal(Importance.High, uriKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedTopicPatternDefault()
    {
        var connector = new Neo4jSourceConnector();
        var topicPatternKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.TopicPatternConfig);
        Assert.Equal(Neo4jConnectorConfig.DefaultTopicPattern, topicPatternKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedDatabaseDefault()
    {
        var connector = new Neo4jSourceConnector();
        var databaseKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.DatabaseConfig);
        Assert.Equal(Neo4jConnectorConfig.DefaultDatabase, databaseKey.DefaultValue);
    }

    [Fact]
    public void Config_IncludeMetadataDefaultsToTrue()
    {
        var connector = new Neo4jSourceConnector();
        var includeMetadataKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.IncludeMetadataConfig);
        Assert.Equal(true, includeMetadataKey.DefaultValue);
    }
}

public class Neo4jSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new Neo4jSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PollAsync_ReturnsEmptyWhenNotStarted()
    {
        using var task = new Neo4jSourceTask();
        var result = await task.PollAsync(CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public async Task CommitAsync_CompletesSuccessfully()
    {
        using var task = new Neo4jSourceTask();
        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void CommitRecord_CompletesSuccessfully()
    {
        using var task = new Neo4jSourceTask();
        var record = new SourceRecord
        {
            Topic = "test",
            Value = [],
            SourcePartition = new Dictionary<string, object>(),
            SourceOffset = new Dictionary<string, object>()
        };
        var metadata = new RecordMetadata
        {
            Topic = "test",
            Partition = 0,
            Offset = 0
        };

        var exception = Record.Exception(() => task.CommitRecord(record, metadata));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new Neo4jSourceTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new Neo4jSourceTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }
}
