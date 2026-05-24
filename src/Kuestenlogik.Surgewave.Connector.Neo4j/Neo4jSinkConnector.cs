using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Neo4j;

/// <summary>
/// Sink connector that writes graph data to Neo4j using Cypher queries.
/// Supports MERGE and CREATE operations with batch transactions.
/// </summary>
public sealed class Neo4jSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(Neo4jSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(Neo4jConnectorConfig.UriConfig, ConfigType.String, Importance.High, "Neo4j connection URI (e.g., bolt://localhost:7687)")
        .Define(Neo4jConnectorConfig.UsernameConfig, ConfigType.String, "", Importance.Medium, "Neo4j username")
        .Define(Neo4jConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.Medium, "Neo4j password")
        .Define(Neo4jConnectorConfig.DatabaseConfig, ConfigType.String, Neo4jConnectorConfig.DefaultDatabase, Importance.Medium, "Neo4j database name")
        .Define(Neo4jConnectorConfig.EncryptedConfig, ConfigType.Boolean, false, Importance.Low, "Enable TLS encryption")
        .Define(Neo4jConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(Neo4jConnectorConfig.LabelConfig, ConfigType.String, "", Importance.High, "Node label (can be overridden by field)")
        .Define(Neo4jConnectorConfig.WriteModeConfig, ConfigType.String, Neo4jConnectorConfig.DefaultWriteMode, Importance.Medium, "Write mode: merge, create", EditorHint.Select, options: ["merge", "create"])
        .Define(Neo4jConnectorConfig.BatchSizeConfig, ConfigType.Int, Neo4jConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for bulk operations")
        .Define(Neo4jConnectorConfig.MaxRetryCountConfig, ConfigType.Int, Neo4jConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Maximum retry attempts")
        .Define(Neo4jConnectorConfig.RetryDelayMsConfig, ConfigType.Int, (int)Neo4jConnectorConfig.DefaultRetryDelayMs, Importance.Low, "Delay between retries in milliseconds")
        .Define(Neo4jConnectorConfig.MergePropertiesConfig, ConfigType.String, "", Importance.Medium, "Comma-separated properties for MERGE key matching")
        .Define(Neo4jConnectorConfig.NodeLabelFieldConfig, ConfigType.String, "", Importance.Low, "Field to use as node label")
        .Define(Neo4jConnectorConfig.IdPropertyConfig, ConfigType.String, "", Importance.Low, "Property to use for MERGE identity")
        .Define(Neo4jConnectorConfig.CustomCypherConfig, ConfigType.String, "", Importance.Low, "Custom Cypher query for writes", EditorHint.Code, "cypher")
        .Define(Neo4jConnectorConfig.UnwindParameterConfig, ConfigType.String, Neo4jConnectorConfig.DefaultUnwindParameter, Importance.Low, "Parameter name for UNWIND in batch operations");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(Neo4jConnectorConfig.UriConfig, out var uri) || string.IsNullOrEmpty(uri))
            throw new ArgumentException($"Required configuration '{Neo4jConnectorConfig.UriConfig}' is missing");

        if (!config.TryGetValue(Neo4jConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{Neo4jConnectorConfig.TopicsConfig}' is missing");

        // Label is required unless custom cypher is provided
        var label = GetConfigValue(config, Neo4jConnectorConfig.LabelConfig, "");
        var customCypher = GetConfigValue(config, Neo4jConnectorConfig.CustomCypherConfig, "");

        if (string.IsNullOrEmpty(label) && string.IsNullOrEmpty(customCypher))
            throw new ArgumentException($"Either '{Neo4jConnectorConfig.LabelConfig}' or '{Neo4jConnectorConfig.CustomCypherConfig}' must be provided");

        _config = new Dictionary<string, string>(config);
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - Neo4j handles transactions internally
        return [new Dictionary<string, string>(_config)];
    }
}
