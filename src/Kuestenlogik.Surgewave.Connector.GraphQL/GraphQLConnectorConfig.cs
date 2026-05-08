namespace Kuestenlogik.Surgewave.Connector.GraphQL;

/// <summary>
/// Configuration constants for GraphQL connectors.
/// </summary>
public static class GraphQLConnectorConfig
{
    // Connection settings
    public const string EndpointConfig = "graphql.endpoint";
    public const string WebSocketEndpointConfig = "graphql.websocket.endpoint";
    public const string AuthHeaderConfig = "graphql.auth.header";
    public const string AuthTokenConfig = "graphql.auth.token";
    public const string HeadersConfig = "graphql.headers";
    public const string TimeoutMsConfig = "graphql.timeout.ms";

    // Source settings
    public const string SourceModeConfig = "graphql.source.mode";
    public const string QueryConfig = "graphql.query";
    public const string OperationNameConfig = "graphql.operation.name";
    public const string VariablesConfig = "graphql.variables";
    public const string PollIntervalMsConfig = "graphql.poll.interval.ms";
    public const string DataPathConfig = "graphql.data.path";
    public const string IdFieldConfig = "graphql.id.field";
    public const string TimestampFieldConfig = "graphql.timestamp.field";
    public const string TopicConfig = "topic";

    // Sink settings
    public const string TopicsConfig = "topics";
    public const string MutationConfig = "graphql.mutation";
    public const string VariablesMappingConfig = "graphql.variables.mapping";
    public const string BatchSizeConfig = "graphql.batch.size";
    public const string MaxRetryCountConfig = "graphql.max.retry.count";
    public const string RetryDelayMsConfig = "graphql.retry.delay.ms";

    // Source modes
    public const string SourceModePoll = "poll";
    public const string SourceModeSubscription = "subscription";

    // Defaults
    public const int DefaultTimeoutMs = 30000;
    public const int DefaultPollIntervalMs = 10000;
    public const int DefaultBatchSize = 100;
    public const int DefaultMaxRetryCount = 3;
    public const int DefaultRetryDelayMs = 1000;
    public const string DefaultAuthHeader = "Authorization";
    public const string DefaultSourceMode = SourceModePoll;

    // Offset keys
    public const string OffsetLastId = "last_id";
    public const string OffsetLastTimestamp = "last_timestamp";
    public const string OffsetLastPoll = "last_poll";
}
