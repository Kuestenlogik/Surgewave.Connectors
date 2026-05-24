namespace Kuestenlogik.Surgewave.Connector.Orleans;

/// <summary>
/// Configuration constants for Orleans Grain Stream connectors.
/// </summary>
public static class OrleansConnectorConfig
{
    // Connection
    public const string ClusterUrl = "orleans.cluster.url";
    public const string DefaultClusterUrl = "localhost:30000";
    public const string ClusterId = "orleans.cluster.id";
    public const string DefaultClusterId = "surgewave-cluster";
    public const string ServiceId = "orleans.service.id";
    public const string DefaultServiceId = "surgewave-service";

    // Streaming
    public const string StreamProvider = "orleans.stream.provider";
    public const string DefaultStreamProvider = "default";
    public const string StreamNamespace = "orleans.stream.namespace";
    public const string StreamId = "orleans.stream.id";

    // Topics
    public const string Topic = "topic";
    public const string Topics = "topics";

    // Batching
    public const string BatchSize = "batch.size";
    public const int DefaultBatchSize = 100;

    // Serialization
    public const string SerializationType = "serialization.type";
    public const string DefaultSerializationType = "json";

    // Publish
    public const string PublishTimeoutMs = "publish.timeout.ms";
    public const int DefaultPublishTimeoutMs = 30000;
    public const string Retries = "retries";
    public const int DefaultRetries = 3;

    // Offset tracking
    public const string OffsetSequenceToken = "sequence_token";
}
