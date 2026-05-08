using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.GraphQL;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.GraphQL.Tests;

public class GraphQLSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsGraphQLSourceTask()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Equal(typeof(GraphQLSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesEndpointConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void Config_DefinesWebSocketEndpointConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.WebSocketEndpointConfig);
    }

    [Fact]
    public void Config_DefinesAuthHeaderConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.AuthHeaderConfig);
    }

    [Fact]
    public void Config_DefinesAuthTokenConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.AuthTokenConfig);
    }

    [Fact]
    public void Config_DefinesHeadersConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.HeadersConfig);
    }

    [Fact]
    public void Config_DefinesTimeoutMsConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.TimeoutMsConfig);
    }

    [Fact]
    public void Config_DefinesSourceModeConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.SourceModeConfig);
    }

    [Fact]
    public void Config_DefinesQueryConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.QueryConfig);
    }

    [Fact]
    public void Config_DefinesOperationNameConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.OperationNameConfig);
    }

    [Fact]
    public void Config_DefinesVariablesConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.VariablesConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalMsConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesDataPathConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.DataPathConfig);
    }

    [Fact]
    public void Config_DefinesIdFieldConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.IdFieldConfig);
    }

    [Fact]
    public void Config_DefinesTimestampFieldConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.TimestampFieldConfig);
    }

    [Fact]
    public void Config_DefinesTopicConfig()
    {
        var connector = new GraphQLSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.TopicConfig);
    }

    [Fact]
    public void Start_ThrowsWhenEndpointMissing()
    {
        var connector = new GraphQLSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.QueryConfig] = "query { users { id } }",
            [GraphQLConnectorConfig.TopicConfig] = "users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GraphQLConnectorConfig.EndpointConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenQueryMissing()
    {
        var connector = new GraphQLSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.TopicConfig] = "users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GraphQLConnectorConfig.QueryConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicMissing()
    {
        var connector = new GraphQLSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.QueryConfig] = "query { users { id } }"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GraphQLConnectorConfig.TopicConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenSubscriptionModeWithoutWebSocketEndpoint()
    {
        var connector = new GraphQLSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.QueryConfig] = "subscription { messageCreated { id } }",
            [GraphQLConnectorConfig.TopicConfig] = "messages",
            [GraphQLConnectorConfig.SourceModeConfig] = GraphQLConnectorConfig.SourceModeSubscription
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GraphQLConnectorConfig.WebSocketEndpointConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidPollConfig()
    {
        var connector = new GraphQLSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.QueryConfig] = "query { users { id } }",
            [GraphQLConnectorConfig.TopicConfig] = "users"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_SucceedsWithValidSubscriptionConfig()
    {
        var connector = new GraphQLSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.WebSocketEndpointConfig] = "wss://api.example.com/graphql",
            [GraphQLConnectorConfig.QueryConfig] = "subscription { messageCreated { id } }",
            [GraphQLConnectorConfig.TopicConfig] = "messages",
            [GraphQLConnectorConfig.SourceModeConfig] = GraphQLConnectorConfig.SourceModeSubscription
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new GraphQLSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.QueryConfig] = "query { users { id } }",
            [GraphQLConnectorConfig.TopicConfig] = "users"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[GraphQLConnectorConfig.EndpointConfig], taskConfigs[0][GraphQLConnectorConfig.EndpointConfig]);
        Assert.Equal(config[GraphQLConnectorConfig.QueryConfig], taskConfigs[0][GraphQLConnectorConfig.QueryConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new GraphQLSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.QueryConfig] = "query { users { id } }",
            [GraphQLConnectorConfig.TopicConfig] = "users"
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
    public void Config_AuthTokenIsPasswordType()
    {
        var connector = new GraphQLSourceConnector();
        var authTokenKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.AuthTokenConfig);
        Assert.Equal(ConfigType.Password, authTokenKey.Type);
    }

    [Fact]
    public void Config_EndpointIsHighImportance()
    {
        var connector = new GraphQLSourceConnector();
        var endpointKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.EndpointConfig);
        Assert.Equal(Importance.High, endpointKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedSourceModeDefault()
    {
        var connector = new GraphQLSourceConnector();
        var sourceModeKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.SourceModeConfig);
        Assert.Equal(GraphQLConnectorConfig.DefaultSourceMode, sourceModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedAuthHeaderDefault()
    {
        var connector = new GraphQLSourceConnector();
        var authHeaderKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.AuthHeaderConfig);
        Assert.Equal(GraphQLConnectorConfig.DefaultAuthHeader, authHeaderKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedTimeoutDefault()
    {
        var connector = new GraphQLSourceConnector();
        var timeoutKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.TimeoutMsConfig);
        Assert.Equal(GraphQLConnectorConfig.DefaultTimeoutMs, timeoutKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new GraphQLSourceConnector();
        var pollIntervalKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.PollIntervalMsConfig);
        Assert.Equal(GraphQLConnectorConfig.DefaultPollIntervalMs, pollIntervalKey.DefaultValue);
    }
}

public class GraphQLSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new GraphQLSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PollAsync_ReturnsEmptyWhenNotStarted()
    {
        using var task = new GraphQLSourceTask();
        var result = await task.PollAsync(CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public async Task CommitAsync_CompletesSuccessfully()
    {
        using var task = new GraphQLSourceTask();
        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void CommitRecord_CompletesSuccessfully()
    {
        using var task = new GraphQLSourceTask();
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
        using var task = new GraphQLSourceTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new GraphQLSourceTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }
}
