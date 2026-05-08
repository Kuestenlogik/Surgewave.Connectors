namespace Kuestenlogik.Surgewave.Connector.Qdrant;

/// <summary>
/// Configuration constants for the Qdrant vector database connector.
/// </summary>
public static class QdrantConnectorConfig
{
    // Connection
    public const string HostConfig = "qdrant.host";
    public const string DefaultHost = "localhost";
    public const string PortConfig = "qdrant.port";
    public const int DefaultPort = 6334;
    public const string HttpsConfig = "qdrant.https";
    public const string ApiKeyConfig = "qdrant.api.key";

    // Topics
    public const string TopicsConfig = "topics";

    // Collection
    public const string CollectionConfig = "collection";
    public const string CreateCollectionConfig = "collection.create";
    public const bool DefaultCreateCollection = true;
    public const string VectorSizeConfig = "vector.size";
    public const int DefaultVectorSize = 1536; // OpenAI text-embedding-3-small default
    public const string DistanceMetricConfig = "distance.metric";
    public const string DistanceCosine = "cosine";
    public const string DistanceEuclid = "euclid";
    public const string DistanceDot = "dot";

    // Fields
    public const string VectorFieldConfig = "vector.field";
    public const string DefaultVectorField = "embedding";
    public const string IdFieldConfig = "id.field";
    public const string IdStrategyConfig = "id.strategy";
    public const string IdStrategyAuto = "auto";
    public const string IdStrategyField = "field";
    public const string IdStrategyKey = "key";
    public const string PayloadFieldsConfig = "payload.fields";

    // Batching
    public const string BatchSizeConfig = "batch.size";
    public const int DefaultBatchSize = 100;

    // Retry
    public const string RetryMaxConfig = "retry.max";
    public const int DefaultRetryMax = 3;
    public const string RetryBackoffMsConfig = "retry.backoff.ms";
    public const int DefaultRetryBackoffMs = 1000;
}
