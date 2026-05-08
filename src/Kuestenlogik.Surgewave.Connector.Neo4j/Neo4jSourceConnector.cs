using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Neo4j;

/// <summary>
/// Source connector that reads graph data from Neo4j using Cypher queries.
/// Supports node polling with label filtering and custom Cypher queries.
/// </summary>
public sealed class Neo4jSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(Neo4jSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(Neo4jConnectorConfig.UriConfig, ConfigType.String, Importance.High, "Neo4j connection URI (e.g., bolt://localhost:7687)")
        .Define(Neo4jConnectorConfig.UsernameConfig, ConfigType.String, "", Importance.Medium, "Neo4j username")
        .Define(Neo4jConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.Medium, "Neo4j password")
        .Define(Neo4jConnectorConfig.DatabaseConfig, ConfigType.String, Neo4jConnectorConfig.DefaultDatabase, Importance.Medium, "Neo4j database name")
        .Define(Neo4jConnectorConfig.EncryptedConfig, ConfigType.Boolean, false, Importance.Low, "Enable TLS encryption")
        .Define(Neo4jConnectorConfig.LabelConfig, ConfigType.String, "", Importance.Medium, "Node label to query (optional if using custom query)")
        .Define(Neo4jConnectorConfig.QueryConfig, ConfigType.String, "", Importance.Medium, "Custom Cypher query (overrides label)", EditorHint.Code, "sql")
        .Define(Neo4jConnectorConfig.TopicConfig, ConfigType.String, "", Importance.Medium, "Destination topic (optional if using pattern)", EditorHint.Topic)
        .Define(Neo4jConnectorConfig.TopicPatternConfig, ConfigType.String, Neo4jConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern")
        .Define(Neo4jConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (int)Neo4jConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in milliseconds")
        .Define(Neo4jConnectorConfig.MaxRowsPerPollConfig, ConfigType.Int, Neo4jConnectorConfig.DefaultMaxRowsPerPoll, Importance.Low, "Max rows per poll")
        .Define(Neo4jConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include Neo4j metadata in output")
        .Define(Neo4jConnectorConfig.TimestampPropertyConfig, ConfigType.String, "", Importance.Low, "Property to use for incremental polling")
        .Define(Neo4jConnectorConfig.IdPropertyConfig, ConfigType.String, "", Importance.Low, "Property to use as record key");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(Neo4jConnectorConfig.UriConfig, out var uri) || string.IsNullOrEmpty(uri))
            throw new ArgumentException($"Required configuration '{Neo4jConnectorConfig.UriConfig}' is missing");

        // Either label or custom query must be provided
        var label = GetConfigValue(config, Neo4jConnectorConfig.LabelConfig, "");
        var query = GetConfigValue(config, Neo4jConnectorConfig.QueryConfig, "");

        if (string.IsNullOrEmpty(label) && string.IsNullOrEmpty(query))
            throw new ArgumentException($"Either '{Neo4jConnectorConfig.LabelConfig}' or '{Neo4jConnectorConfig.QueryConfig}' must be provided");

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
        // Single task - Neo4j handles distributed queries internally
        return [new Dictionary<string, string>(_config)];
    }
}
