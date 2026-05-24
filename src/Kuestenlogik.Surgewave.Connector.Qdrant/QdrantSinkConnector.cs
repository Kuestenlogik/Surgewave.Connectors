namespace Kuestenlogik.Surgewave.Connector.Qdrant;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that stores vector embeddings in Qdrant.
/// Designed to work with the OpenAI connector in a pipeline:
/// Topic -> OpenAI (embeddings) -> Webhook -> Topic -> Qdrant
/// </summary>
public sealed class QdrantSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(QdrantSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(QdrantConnectorConfig.HostConfig, ConfigType.String, QdrantConnectorConfig.DefaultHost, Importance.High,
            "Qdrant server hostname")
        .Define(QdrantConnectorConfig.PortConfig, ConfigType.Int, (long)QdrantConnectorConfig.DefaultPort, Importance.High,
            "Qdrant gRPC port (default: 6334)")
        .Define(QdrantConnectorConfig.HttpsConfig, ConfigType.Boolean, false, Importance.Medium,
            "Use HTTPS/TLS for connection")
        .Define(QdrantConnectorConfig.ApiKeyConfig, ConfigType.Password, "", Importance.Medium,
            "Qdrant API key (optional)")
        // Topics
        .Define(QdrantConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics containing vector data", EditorHint.Topic)
        // Collection
        .Define(QdrantConnectorConfig.CollectionConfig, ConfigType.String, Importance.High,
            "Qdrant collection name")
        .Define(QdrantConnectorConfig.CreateCollectionConfig, ConfigType.Boolean, QdrantConnectorConfig.DefaultCreateCollection, Importance.Medium,
            "Auto-create collection if it doesn't exist")
        .Define(QdrantConnectorConfig.VectorSizeConfig, ConfigType.Int, (long)QdrantConnectorConfig.DefaultVectorSize, Importance.Medium,
            "Vector dimensions (default: 1536 for OpenAI text-embedding-3-small)")
        .Define(QdrantConnectorConfig.DistanceMetricConfig, ConfigType.String, QdrantConnectorConfig.DistanceCosine, Importance.Medium,
            "Distance metric: 'cosine', 'euclid', or 'dot'", EditorHint.Select, options: ["Cosine", "Euclid", "Dot"])
        // Fields
        .Define(QdrantConnectorConfig.VectorFieldConfig, ConfigType.String, QdrantConnectorConfig.DefaultVectorField, Importance.Medium,
            "JSON field containing the vector embedding array")
        .Define(QdrantConnectorConfig.IdFieldConfig, ConfigType.String, "", Importance.Medium,
            "JSON field to use as point ID (optional)")
        .Define(QdrantConnectorConfig.IdStrategyConfig, ConfigType.String, QdrantConnectorConfig.IdStrategyAuto, Importance.Medium,
            "ID strategy: 'auto' (UUID), 'field' (from id.field), or 'key' (from message key)", EditorHint.Select, options: ["auto", "field", "key"])
        .Define(QdrantConnectorConfig.PayloadFieldsConfig, ConfigType.String, "", Importance.Medium,
            "Comma-separated list of JSON fields to include in payload (empty = all fields except vector)")
        // Batching
        .Define(QdrantConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)QdrantConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of points to batch before upserting")
        // Retry
        .Define(QdrantConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)QdrantConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(QdrantConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)QdrantConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(QdrantConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {QdrantConnectorConfig.TopicsConfig}");

        if (!config.TryGetValue(QdrantConnectorConfig.CollectionConfig, out _))
            throw new ArgumentException($"Missing required config: {QdrantConnectorConfig.CollectionConfig}");

        // Validate distance metric
        if (config.TryGetValue(QdrantConnectorConfig.DistanceMetricConfig, out var metric))
        {
            if (metric is not (QdrantConnectorConfig.DistanceCosine
                            or QdrantConnectorConfig.DistanceEuclid
                            or QdrantConnectorConfig.DistanceDot))
            {
                throw new ArgumentException(
                    $"Invalid distance metric '{metric}'. Must be 'cosine', 'euclid', or 'dot'");
            }
        }

        // Validate ID strategy
        if (config.TryGetValue(QdrantConnectorConfig.IdStrategyConfig, out var strategy))
        {
            if (strategy is not (QdrantConnectorConfig.IdStrategyAuto
                              or QdrantConnectorConfig.IdStrategyField
                              or QdrantConnectorConfig.IdStrategyKey))
            {
                throw new ArgumentException(
                    $"Invalid ID strategy '{strategy}'. Must be 'auto', 'field', or 'key'");
            }

            // If using field strategy, id.field must be specified
            if (strategy == QdrantConnectorConfig.IdStrategyField)
            {
                if (!config.TryGetValue(QdrantConnectorConfig.IdFieldConfig, out var idField)
                    || string.IsNullOrEmpty(idField))
                {
                    throw new ArgumentException(
                        $"ID strategy 'field' requires '{QdrantConnectorConfig.IdFieldConfig}' to be specified");
                }
            }
        }

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - Qdrant handles concurrent writes
        return [new Dictionary<string, string>(_config)];
    }
}
