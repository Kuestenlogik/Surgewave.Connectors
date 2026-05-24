using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;

/// <summary>
/// Sink connector that writes records to Azure Cosmos DB containers.
/// Supports upsert, create, replace, and delete operations with bulk execution.
/// </summary>
public sealed class CosmosDbSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(CosmosDbSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(CosmosDbConnectorConfig.ConnectionStringConfig, ConfigType.Password, "", Importance.High, "Cosmos DB connection string (preferred)")
        .Define(CosmosDbConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.High, "Cosmos DB endpoint URL (alternative to connection string)")
        .Define(CosmosDbConnectorConfig.AccountKeyConfig, ConfigType.Password, "", Importance.High, "Cosmos DB account key (used with endpoint)")
        .Define(CosmosDbConnectorConfig.DatabaseConfig, ConfigType.String, Importance.High, "Database name")
        .Define(CosmosDbConnectorConfig.ContainerConfig, ConfigType.String, Importance.High, "Container name to write to")
        .Define(CosmosDbConnectorConfig.TopicsConfig, ConfigType.String, Importance.High, "Surgewave topics to consume (comma-separated)", EditorHint.Topic)
        .Define(CosmosDbConnectorConfig.PartitionKeyPathConfig, ConfigType.String, "/id", Importance.High, "Partition key path (e.g., /partitionKey)")
        .Define(CosmosDbConnectorConfig.IdFieldConfig, ConfigType.String, "id", Importance.Medium, "Field to use as document id")
        .Define(CosmosDbConnectorConfig.WriteModeConfig, ConfigType.String, CosmosDbConnectorConfig.WriteModeUpsert, Importance.Medium, "Write mode: upsert, create, replace, delete", EditorHint.Select, options: ["upsert", "create", "replace", "delete"])
        .Define(CosmosDbConnectorConfig.BatchSizeConfig, ConfigType.Int, CosmosDbConnectorConfig.DefaultBatchSize, Importance.Low, "Batch size for bulk operations")
        .Define(CosmosDbConnectorConfig.AutoCreateContainerConfig, ConfigType.Boolean, false, Importance.Low, "Auto-create container if not exists")
        .Define(CosmosDbConnectorConfig.ThroughputConfig, ConfigType.Int, CosmosDbConnectorConfig.DefaultThroughput, Importance.Low, "Throughput (RU/s) for auto-created container")
        .Define(CosmosDbConnectorConfig.MaxRetryCountConfig, ConfigType.Int, CosmosDbConnectorConfig.DefaultMaxRetryCount, Importance.Low, "Max retry count for transient failures")
        .Define(CosmosDbConnectorConfig.MaxRetryWaitTimeMsConfig, ConfigType.Int, (int)CosmosDbConnectorConfig.DefaultMaxRetryWaitTimeMs, Importance.Low, "Max retry wait time in ms");

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

        if (!config.TryGetValue(CosmosDbConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Required configuration '{CosmosDbConnectorConfig.TopicsConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
        // Connector-level stop
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for simplicity
        return [new Dictionary<string, string>(_config)];
    }
}
