using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.GraphQL;

/// <summary>
/// Sink connector for GraphQL APIs.
/// Executes GraphQL mutations for each incoming record.
/// </summary>
public sealed class GraphQLSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(GraphQLSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Connection settings
        .Define(GraphQLConnectorConfig.EndpointConfig, ConfigType.String, Importance.High,
            "GraphQL HTTP endpoint URL")
        .Define(GraphQLConnectorConfig.AuthHeaderConfig, ConfigType.String, GraphQLConnectorConfig.DefaultAuthHeader, Importance.Low,
            "Authentication header name (default: Authorization)")
        .Define(GraphQLConnectorConfig.AuthTokenConfig, ConfigType.Password, "", Importance.High,
            "Authentication token (Bearer token, API key, etc.)")
        .Define(GraphQLConnectorConfig.HeadersConfig, ConfigType.String, "", Importance.Low,
            "Additional HTTP headers as key=value pairs separated by semicolons", EditorHint.Multiline)
        .Define(GraphQLConnectorConfig.TimeoutMsConfig, ConfigType.Int, GraphQLConnectorConfig.DefaultTimeoutMs, Importance.Low,
            "Request timeout in milliseconds")

        // Sink settings
        .Define(GraphQLConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(GraphQLConnectorConfig.MutationConfig, ConfigType.String, Importance.High,
            "GraphQL mutation template with $input variable placeholder")
        .Define(GraphQLConnectorConfig.OperationNameConfig, ConfigType.String, "", Importance.Low,
            "Operation name for mutations with multiple operations")
        .Define(GraphQLConnectorConfig.VariablesMappingConfig, ConfigType.String, "", Importance.Low,
            "Variable mapping as field=jsonPath pairs separated by semicolons")
        .Define(GraphQLConnectorConfig.BatchSizeConfig, ConfigType.Int, GraphQLConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch in a single mutation")
        .Define(GraphQLConnectorConfig.MaxRetryCountConfig, ConfigType.Int, GraphQLConnectorConfig.DefaultMaxRetryCount, Importance.Low,
            "Maximum retry attempts for failed mutations")
        .Define(GraphQLConnectorConfig.RetryDelayMsConfig, ConfigType.Int, GraphQLConnectorConfig.DefaultRetryDelayMs, Importance.Low,
            "Delay between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Validate required settings
        if (!config.TryGetValue(GraphQLConnectorConfig.EndpointConfig, out var endpoint) ||
            string.IsNullOrWhiteSpace(endpoint))
        {
            throw new ArgumentException($"Missing required config: {GraphQLConnectorConfig.EndpointConfig}");
        }

        if (!config.TryGetValue(GraphQLConnectorConfig.MutationConfig, out var mutation) ||
            string.IsNullOrWhiteSpace(mutation))
        {
            throw new ArgumentException($"Missing required config: {GraphQLConnectorConfig.MutationConfig}");
        }

        if (!config.TryGetValue(GraphQLConnectorConfig.TopicsConfig, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"Missing required config: {GraphQLConnectorConfig.TopicsConfig}");
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // GraphQL sink only supports a single task
        return [new Dictionary<string, string>(_config)];
    }
}
