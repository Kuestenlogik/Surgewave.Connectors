using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;

/// <summary>
/// Source connector that captures changes from Azure Cosmos DB using Change Feed.
/// Reads create, replace, and delete events and produces CDC-compatible output.
/// </summary>
public sealed class CosmosDbSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(CosmosDbSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(CosmosDbConnectorConfig.ConnectionStringConfig, ConfigType.Password, "", Importance.High, "Cosmos DB connection string (preferred)")
        .Define(CosmosDbConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.High, "Cosmos DB endpoint URL (alternative to connection string)")
        .Define(CosmosDbConnectorConfig.AccountKeyConfig, ConfigType.Password, "", Importance.High, "Cosmos DB account key (used with endpoint)")
        .Define(CosmosDbConnectorConfig.DatabaseConfig, ConfigType.String, Importance.High, "Database name")
        .Define(CosmosDbConnectorConfig.ContainerConfig, ConfigType.String, Importance.High, "Container name to monitor")
        .Define(CosmosDbConnectorConfig.TopicPatternConfig, ConfigType.String, CosmosDbConnectorConfig.DefaultTopicPattern, Importance.Medium, "Topic naming pattern (${database}, ${container})")
        .Define(CosmosDbConnectorConfig.ChangeFeedStartFromConfig, ConfigType.String, CosmosDbConnectorConfig.StartFromNow, Importance.Medium, "Start from: beginning, now, continuation", EditorHint.Select, options: ["beginning", "now", "continuation"])
        .Define(CosmosDbConnectorConfig.ChangeFeedMaxItemsConfig, ConfigType.Int, CosmosDbConnectorConfig.DefaultChangeFeedMaxItems, Importance.Low, "Max items per change feed batch")
        .Define(CosmosDbConnectorConfig.ChangeFeedPollIntervalMsConfig, ConfigType.Int, (int)CosmosDbConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in ms")
        .Define(CosmosDbConnectorConfig.LeaseContainerConfig, ConfigType.String, "", Importance.Low, "Lease container name (auto-created if not specified)")
        .Define(CosmosDbConnectorConfig.LeaseContainerPrefixConfig, ConfigType.String, CosmosDbConnectorConfig.DefaultLeaseContainerPrefix, Importance.Low, "Lease prefix for this connector instance")
        .Define(CosmosDbConnectorConfig.IncludeMetadataConfig, ConfigType.Boolean, true, Importance.Low, "Include Cosmos DB metadata in output");

    public override void Start(IDictionary<string, string> config)
    {
        var hasConnectionString = config.TryGetValue(CosmosDbConnectorConfig.ConnectionStringConfig, out var connStr) && !string.IsNullOrEmpty(connStr);
        var hasEndpoint = config.TryGetValue(CosmosDbConnectorConfig.EndpointConfig, out var endpoint) && !string.IsNullOrEmpty(endpoint);

        if (!hasConnectionString && !hasEndpoint)
            throw new ArgumentException($"Either '{CosmosDbConnectorConfig.ConnectionStringConfig}' or '{CosmosDbConnectorConfig.EndpointConfig}' must be specified");

        if (!config.TryGetValue(CosmosDbConnectorConfig.DatabaseConfig, out var database) || string.IsNullOrEmpty(database))
            throw new ArgumentException($"Required configuration '{CosmosDbConnectorConfig.DatabaseConfig}' is missing");

        if (!config.TryGetValue(CosmosDbConnectorConfig.ContainerConfig, out var container) || string.IsNullOrEmpty(container))
            throw new ArgumentException($"Required configuration '{CosmosDbConnectorConfig.ContainerConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - change feed processor handles partitions internally
        return [new Dictionary<string, string>(_config)];
    }
}
