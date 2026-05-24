using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Neo4j;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Neo4j.Tests;

public class Neo4jSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsNeo4jSinkTask()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Equal(typeof(Neo4jSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesUriConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.UriConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesDatabaseConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.DatabaseConfig);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesLabelConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.LabelConfig);
    }

    [Fact]
    public void Config_DefinesWriteModeConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.WriteModeConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesMaxRetryCountConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void Config_DefinesRetryDelayMsConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Config_DefinesMergePropertiesConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.MergePropertiesConfig);
    }

    [Fact]
    public void Config_DefinesNodeLabelFieldConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.NodeLabelFieldConfig);
    }

    [Fact]
    public void Config_DefinesCustomCypherConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.CustomCypherConfig);
    }

    [Fact]
    public void Config_DefinesUnwindParameterConfig()
    {
        var connector = new Neo4jSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == Neo4jConnectorConfig.UnwindParameterConfig);
    }

    [Fact]
    public void Start_ThrowsWhenUriMissing()
    {
        var connector = new Neo4jSinkConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.TopicsConfig] = "test-topic",
            [Neo4jConnectorConfig.LabelConfig] = "Person"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(Neo4jConnectorConfig.UriConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new Neo4jSinkConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.LabelConfig] = "Person"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(Neo4jConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenLabelAndCustomCypherBothMissing()
    {
        var connector = new Neo4jSinkConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(Neo4jConnectorConfig.LabelConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var connector = new Neo4jSinkConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.TopicsConfig] = "test-topic",
            [Neo4jConnectorConfig.LabelConfig] = "Person"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_SucceedsWithCustomCypher()
    {
        var connector = new Neo4jSinkConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.TopicsConfig] = "test-topic",
            [Neo4jConnectorConfig.CustomCypherConfig] = "UNWIND $events AS event MERGE (n:Person {id: event.id}) SET n += event"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new Neo4jSinkConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.TopicsConfig] = "test-topic",
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
        var connector = new Neo4jSinkConnector();
        var config = new Dictionary<string, string>
        {
            [Neo4jConnectorConfig.UriConfig] = "bolt://localhost:7687",
            [Neo4jConnectorConfig.TopicsConfig] = "test-topic",
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
        var connector = new Neo4jSinkConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_UriIsHighImportance()
    {
        var connector = new Neo4jSinkConnector();
        var uriKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.UriConfig);
        Assert.Equal(Importance.High, uriKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedBatchSizeDefault()
    {
        var connector = new Neo4jSinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.BatchSizeConfig);
        Assert.Equal(Neo4jConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRetryCountDefault()
    {
        var connector = new Neo4jSinkConnector();
        var maxRetryKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.MaxRetryCountConfig);
        Assert.Equal(Neo4jConnectorConfig.DefaultMaxRetryCount, maxRetryKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedWriteModeDefault()
    {
        var connector = new Neo4jSinkConnector();
        var writeModeKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.WriteModeConfig);
        Assert.Equal(Neo4jConnectorConfig.DefaultWriteMode, writeModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedUnwindParameterDefault()
    {
        var connector = new Neo4jSinkConnector();
        var unwindKey = connector.Config.Keys.First(k => k.Name == Neo4jConnectorConfig.UnwindParameterConfig);
        Assert.Equal(Neo4jConnectorConfig.DefaultUnwindParameter, unwindKey.DefaultValue);
    }
}

public class Neo4jSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new Neo4jSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PutAsync_SkipsNullValues()
    {
        using var task = new Neo4jSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = null! }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_CompletesSuccessfully()
    {
        using var task = new Neo4jSinkTask();
        var offsets = new Dictionary<TopicPartition, long>();

        var exception = await Record.ExceptionAsync(() => task.FlushAsync(offsets, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new Neo4jSinkTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new Neo4jSinkTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_HandlesInvalidJson()
    {
        using var task = new Neo4jSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = System.Text.Encoding.UTF8.GetBytes("not valid json") }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_HandlesEmptyRecordsList()
    {
        using var task = new Neo4jSinkTask();
        var records = new List<SinkRecord>();

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }
}
