using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.GraphQL;

/// <summary>
/// Source connector for GraphQL APIs.
/// Supports two modes:
/// - Poll: Execute GraphQL queries at configurable intervals
/// - Subscription: Connect to GraphQL subscriptions via WebSocket
/// </summary>
public sealed class GraphQLSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(GraphQLSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Connection settings
        .Define(GraphQLConnectorConfig.EndpointConfig, ConfigType.String, Importance.High,
            "GraphQL HTTP endpoint URL")
        .Define(GraphQLConnectorConfig.WebSocketEndpointConfig, ConfigType.String, "", Importance.Medium,
            "WebSocket endpoint for subscriptions (ws:// or wss://)")
        .Define(GraphQLConnectorConfig.AuthHeaderConfig, ConfigType.String, GraphQLConnectorConfig.DefaultAuthHeader, Importance.Low,
            "Authentication header name (default: Authorization)")
        .Define(GraphQLConnectorConfig.AuthTokenConfig, ConfigType.Password, "", Importance.High,
            "Authentication token (Bearer token, API key, etc.)")
        .Define(GraphQLConnectorConfig.HeadersConfig, ConfigType.String, "", Importance.Low,
            "Additional HTTP headers as key=value pairs separated by semicolons", EditorHint.Multiline)
        .Define(GraphQLConnectorConfig.TimeoutMsConfig, ConfigType.Int, GraphQLConnectorConfig.DefaultTimeoutMs, Importance.Low,
            "Request timeout in milliseconds")

        // Source settings
        .Define(GraphQLConnectorConfig.SourceModeConfig, ConfigType.String, GraphQLConnectorConfig.DefaultSourceMode, Importance.Medium,
            "Source mode: 'poll' (query at intervals) or 'subscription' (WebSocket)")
        .Define(GraphQLConnectorConfig.QueryConfig, ConfigType.String, Importance.High,
            "GraphQL query or subscription string", EditorHint.Code, "sql")
        .Define(GraphQLConnectorConfig.OperationNameConfig, ConfigType.String, "", Importance.Low,
            "Operation name for queries with multiple operations")
        .Define(GraphQLConnectorConfig.VariablesConfig, ConfigType.String, "", Importance.Low,
            "Query variables as JSON object")
        .Define(GraphQLConnectorConfig.PollIntervalMsConfig, ConfigType.Int, GraphQLConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds (poll mode only)")
        .Define(GraphQLConnectorConfig.DataPathConfig, ConfigType.String, "", Importance.Medium,
            "JSON path to data array in response (e.g., 'users' for data.users)")
        .Define(GraphQLConnectorConfig.IdFieldConfig, ConfigType.String, "", Importance.Low,
            "Field to use as record key")
        .Define(GraphQLConnectorConfig.TimestampFieldConfig, ConfigType.String, "", Importance.Low,
            "Field for incremental polling (must be sortable)")
        .Define(GraphQLConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination topic for records", EditorHint.Topic);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Validate required settings
        if (!config.TryGetValue(GraphQLConnectorConfig.EndpointConfig, out var endpoint) ||
            string.IsNullOrWhiteSpace(endpoint))
        {
            throw new ArgumentException($"Missing required config: {GraphQLConnectorConfig.EndpointConfig}");
        }

        if (!config.TryGetValue(GraphQLConnectorConfig.QueryConfig, out var query) ||
            string.IsNullOrWhiteSpace(query))
        {
            throw new ArgumentException($"Missing required config: {GraphQLConnectorConfig.QueryConfig}");
        }

        if (!config.TryGetValue(GraphQLConnectorConfig.TopicConfig, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"Missing required config: {GraphQLConnectorConfig.TopicConfig}");
        }

        // Validate subscription mode requirements
        var mode = config.TryGetValue(GraphQLConnectorConfig.SourceModeConfig, out var m)
            ? m : GraphQLConnectorConfig.DefaultSourceMode;

        if (mode == GraphQLConnectorConfig.SourceModeSubscription)
        {
            if (!config.TryGetValue(GraphQLConnectorConfig.WebSocketEndpointConfig, out var wsEndpoint) ||
                string.IsNullOrWhiteSpace(wsEndpoint))
            {
                throw new ArgumentException(
                    $"Subscription mode requires {GraphQLConnectorConfig.WebSocketEndpointConfig}");
            }
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // GraphQL source only supports a single task
        return [new Dictionary<string, string>(_config)];
    }
}
