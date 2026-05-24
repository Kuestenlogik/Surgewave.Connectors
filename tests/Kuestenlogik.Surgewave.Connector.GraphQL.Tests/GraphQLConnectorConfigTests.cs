using Kuestenlogik.Surgewave.Connector.GraphQL;

namespace Kuestenlogik.Surgewave.Connector.GraphQL.Tests;

public class GraphQLConnectorConfigTests
{
    [Fact]
    public void EndpointConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.endpoint", GraphQLConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void WebSocketEndpointConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.websocket.endpoint", GraphQLConnectorConfig.WebSocketEndpointConfig);
    }

    [Fact]
    public void AuthHeaderConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.auth.header", GraphQLConnectorConfig.AuthHeaderConfig);
    }

    [Fact]
    public void AuthTokenConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.auth.token", GraphQLConnectorConfig.AuthTokenConfig);
    }

    [Fact]
    public void HeadersConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.headers", GraphQLConnectorConfig.HeadersConfig);
    }

    [Fact]
    public void TimeoutMsConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.timeout.ms", GraphQLConnectorConfig.TimeoutMsConfig);
    }

    [Fact]
    public void SourceModeConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.source.mode", GraphQLConnectorConfig.SourceModeConfig);
    }

    [Fact]
    public void QueryConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.query", GraphQLConnectorConfig.QueryConfig);
    }

    [Fact]
    public void OperationNameConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.operation.name", GraphQLConnectorConfig.OperationNameConfig);
    }

    [Fact]
    public void VariablesConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.variables", GraphQLConnectorConfig.VariablesConfig);
    }

    [Fact]
    public void PollIntervalMsConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.poll.interval.ms", GraphQLConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void DataPathConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.data.path", GraphQLConnectorConfig.DataPathConfig);
    }

    [Fact]
    public void IdFieldConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.id.field", GraphQLConnectorConfig.IdFieldConfig);
    }

    [Fact]
    public void TimestampFieldConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.timestamp.field", GraphQLConnectorConfig.TimestampFieldConfig);
    }

    [Fact]
    public void TopicConfig_HasExpectedValue()
    {
        Assert.Equal("topic", GraphQLConnectorConfig.TopicConfig);
    }

    [Fact]
    public void TopicsConfig_HasExpectedValue()
    {
        Assert.Equal("topics", GraphQLConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void MutationConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.mutation", GraphQLConnectorConfig.MutationConfig);
    }

    [Fact]
    public void VariablesMappingConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.variables.mapping", GraphQLConnectorConfig.VariablesMappingConfig);
    }

    [Fact]
    public void BatchSizeConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.batch.size", GraphQLConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void MaxRetryCountConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.max.retry.count", GraphQLConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void RetryDelayMsConfig_HasExpectedValue()
    {
        Assert.Equal("graphql.retry.delay.ms", GraphQLConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void SourceModePoll_HasExpectedValue()
    {
        Assert.Equal("poll", GraphQLConnectorConfig.SourceModePoll);
    }

    [Fact]
    public void SourceModeSubscription_HasExpectedValue()
    {
        Assert.Equal("subscription", GraphQLConnectorConfig.SourceModeSubscription);
    }

    [Fact]
    public void DefaultTimeoutMs_HasExpectedValue()
    {
        Assert.Equal(30000, GraphQLConnectorConfig.DefaultTimeoutMs);
    }

    [Fact]
    public void DefaultPollIntervalMs_HasExpectedValue()
    {
        Assert.Equal(10000, GraphQLConnectorConfig.DefaultPollIntervalMs);
    }

    [Fact]
    public void DefaultBatchSize_HasExpectedValue()
    {
        Assert.Equal(100, GraphQLConnectorConfig.DefaultBatchSize);
    }

    [Fact]
    public void DefaultMaxRetryCount_HasExpectedValue()
    {
        Assert.Equal(3, GraphQLConnectorConfig.DefaultMaxRetryCount);
    }

    [Fact]
    public void DefaultRetryDelayMs_HasExpectedValue()
    {
        Assert.Equal(1000, GraphQLConnectorConfig.DefaultRetryDelayMs);
    }

    [Fact]
    public void DefaultAuthHeader_HasExpectedValue()
    {
        Assert.Equal("Authorization", GraphQLConnectorConfig.DefaultAuthHeader);
    }

    [Fact]
    public void DefaultSourceMode_HasExpectedValue()
    {
        Assert.Equal("poll", GraphQLConnectorConfig.DefaultSourceMode);
    }

    [Fact]
    public void OffsetLastId_HasExpectedValue()
    {
        Assert.Equal("last_id", GraphQLConnectorConfig.OffsetLastId);
    }

    [Fact]
    public void OffsetLastTimestamp_HasExpectedValue()
    {
        Assert.Equal("last_timestamp", GraphQLConnectorConfig.OffsetLastTimestamp);
    }

    [Fact]
    public void OffsetLastPoll_HasExpectedValue()
    {
        Assert.Equal("last_poll", GraphQLConnectorConfig.OffsetLastPoll);
    }
}
