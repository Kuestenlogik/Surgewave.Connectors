using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.GraphQL;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.GraphQL.Tests;

public class GraphQLSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsGraphQLSinkTask()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Equal(typeof(GraphQLSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesEndpointConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void Config_DefinesAuthHeaderConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.AuthHeaderConfig);
    }

    [Fact]
    public void Config_DefinesAuthTokenConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.AuthTokenConfig);
    }

    [Fact]
    public void Config_DefinesHeadersConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.HeadersConfig);
    }

    [Fact]
    public void Config_DefinesTimeoutMsConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.TimeoutMsConfig);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesMutationConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.MutationConfig);
    }

    [Fact]
    public void Config_DefinesOperationNameConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.OperationNameConfig);
    }

    [Fact]
    public void Config_DefinesVariablesMappingConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.VariablesMappingConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesMaxRetryCountConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void Config_DefinesRetryDelayMsConfig()
    {
        var connector = new GraphQLSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GraphQLConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenEndpointMissing()
    {
        var connector = new GraphQLSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.MutationConfig] = "mutation($input: UserInput!) { createUser(input: $input) { id } }",
            [GraphQLConnectorConfig.TopicsConfig] = "users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GraphQLConnectorConfig.EndpointConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenMutationMissing()
    {
        var connector = new GraphQLSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.TopicsConfig] = "users"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GraphQLConnectorConfig.MutationConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new GraphQLSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.MutationConfig] = "mutation($input: UserInput!) { createUser(input: $input) { id } }"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GraphQLConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var connector = new GraphQLSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.MutationConfig] = "mutation($input: UserInput!) { createUser(input: $input) { id } }",
            [GraphQLConnectorConfig.TopicsConfig] = "users"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_SucceedsWithBatchMutation()
    {
        var connector = new GraphQLSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.MutationConfig] = "mutation($inputs: [UserInput!]!) { createUsers(inputs: $inputs) { id } }",
            [GraphQLConnectorConfig.TopicsConfig] = "users"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new GraphQLSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.MutationConfig] = "mutation($input: UserInput!) { createUser(input: $input) { id } }",
            [GraphQLConnectorConfig.TopicsConfig] = "users"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[GraphQLConnectorConfig.EndpointConfig], taskConfigs[0][GraphQLConnectorConfig.EndpointConfig]);
        Assert.Equal(config[GraphQLConnectorConfig.MutationConfig], taskConfigs[0][GraphQLConnectorConfig.MutationConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new GraphQLSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GraphQLConnectorConfig.EndpointConfig] = "https://api.example.com/graphql",
            [GraphQLConnectorConfig.MutationConfig] = "mutation($input: UserInput!) { createUser(input: $input) { id } }",
            [GraphQLConnectorConfig.TopicsConfig] = "users"
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
        var connector = new GraphQLSinkConnector();
        var authTokenKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.AuthTokenConfig);
        Assert.Equal(ConfigType.Password, authTokenKey.Type);
    }

    [Fact]
    public void Config_EndpointIsHighImportance()
    {
        var connector = new GraphQLSinkConnector();
        var endpointKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.EndpointConfig);
        Assert.Equal(Importance.High, endpointKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedBatchSizeDefault()
    {
        var connector = new GraphQLSinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.BatchSizeConfig);
        Assert.Equal(GraphQLConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRetryCountDefault()
    {
        var connector = new GraphQLSinkConnector();
        var maxRetryKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.MaxRetryCountConfig);
        Assert.Equal(GraphQLConnectorConfig.DefaultMaxRetryCount, maxRetryKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedRetryDelayDefault()
    {
        var connector = new GraphQLSinkConnector();
        var retryDelayKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.RetryDelayMsConfig);
        Assert.Equal(GraphQLConnectorConfig.DefaultRetryDelayMs, retryDelayKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedTimeoutDefault()
    {
        var connector = new GraphQLSinkConnector();
        var timeoutKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.TimeoutMsConfig);
        Assert.Equal(GraphQLConnectorConfig.DefaultTimeoutMs, timeoutKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedAuthHeaderDefault()
    {
        var connector = new GraphQLSinkConnector();
        var authHeaderKey = connector.Config.Keys.First(k => k.Name == GraphQLConnectorConfig.AuthHeaderConfig);
        Assert.Equal(GraphQLConnectorConfig.DefaultAuthHeader, authHeaderKey.DefaultValue);
    }
}

public class GraphQLSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new GraphQLSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PutAsync_SkipsNullValues()
    {
        using var task = new GraphQLSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = null! }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_SkipsEmptyValues()
    {
        using var task = new GraphQLSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = [] }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_CompletesSuccessfully()
    {
        using var task = new GraphQLSinkTask();
        var offsets = new Dictionary<TopicPartition, long>();

        var exception = await Record.ExceptionAsync(() => task.FlushAsync(offsets, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new GraphQLSinkTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new GraphQLSinkTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_HandlesEmptyRecordsList()
    {
        using var task = new GraphQLSinkTask();
        var records = new List<SinkRecord>();

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }
}
