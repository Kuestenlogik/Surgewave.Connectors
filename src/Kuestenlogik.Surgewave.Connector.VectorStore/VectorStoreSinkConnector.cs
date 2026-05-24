using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.VectorStore;

/// <summary>
/// Sink connector that stores embeddings and metadata in an embedded vector store.
/// Consumes JSON records containing embedding vectors and optional content/metadata,
/// and upserts them into a named in-memory vector store collection.
/// </summary>
[ConnectorMetadata(
    Name = "Vector Store Sink",
    Description = "Store embeddings and metadata in an embedded vector store",
    Author = "KL Surgewave",
    Tags = "vector,embeddings,ai,storage,sink",
    Icon = "DataArray")]
public sealed class VectorStoreSinkConnector : SinkConnector
{
    internal const string CollectionNameConfig = "collection.name";
    internal const string TopicsConfig = "topics";
    internal const string EmbeddingFieldConfig = "embedding.field";
    internal const string ContentFieldConfig = "content.field";
    internal const string IdFieldConfig = "id.field";
    internal const string PersistenceTopicConfig = "persistence.topic";

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(VectorStoreSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(CollectionNameConfig, ConfigType.String, Importance.High, "Collection name for the vector store")
        .Define(TopicsConfig, ConfigType.String, Importance.High, "Topics to consume from")
        .Define(EmbeddingFieldConfig, ConfigType.String, "embedding", Importance.Medium, "JSON field containing the float[] embedding vector")
        .Define(ContentFieldConfig, ConfigType.String, "content", Importance.Medium, "JSON field containing the text content")
        .Define(IdFieldConfig, ConfigType.String, "id", Importance.Medium, "JSON field for the document ID (falls back to record key)")
        .Define(PersistenceTopicConfig, ConfigType.String, "", Importance.Low, "If set, persist vectors to a compacted topic for durability");

    private string _collectionName = "";
    private string _topics = "";
    private string _embeddingField = "embedding";
    private string _contentField = "content";
    private string _idField = "id";
    private string _persistenceTopic = "";

    public override void Start(IDictionary<string, string> config)
    {
        _collectionName = config.TryGetValue(CollectionNameConfig, out var collection)
            ? collection
            : throw new ArgumentException($"Missing required config: {CollectionNameConfig}");

        _topics = config.TryGetValue(TopicsConfig, out var topics)
            ? topics
            : throw new ArgumentException($"Missing required config: {TopicsConfig}");

        if (config.TryGetValue(EmbeddingFieldConfig, out var embeddingField))
        {
            _embeddingField = embeddingField;
        }

        if (config.TryGetValue(ContentFieldConfig, out var contentField))
        {
            _contentField = contentField;
        }

        if (config.TryGetValue(IdFieldConfig, out var idField))
        {
            _idField = idField;
        }

        if (config.TryGetValue(PersistenceTopicConfig, out var persistenceTopic))
        {
            _persistenceTopic = persistenceTopic;
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return
        [
            new Dictionary<string, string>
            {
                [CollectionNameConfig] = _collectionName,
                [TopicsConfig] = _topics,
                [EmbeddingFieldConfig] = _embeddingField,
                [ContentFieldConfig] = _contentField,
                [IdFieldConfig] = _idField,
                [PersistenceTopicConfig] = _persistenceTopic
            }
        ];
    }
}
