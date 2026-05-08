using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.VectorStore;

/// <summary>
/// Source connector that queries an embedded vector store by similarity search.
/// Reads query vectors from an input topic and produces search results to an output topic.
/// </summary>
[ConnectorMetadata(
    Name = "Vector Store Source",
    Description = "Query an embedded vector store by similarity search",
    Author = "KL Surgewave",
    Tags = "vector,embeddings,ai,search,source",
    Icon = "Search")]
public sealed class VectorStoreSourceConnector : SourceConnector
{
    internal const string CollectionNameConfig = "collection.name";
    internal const string TopicConfig = "topic";
    internal const string QueryTopicConfig = "query.topic";
    internal const string TopKConfig = "top.k";
    internal const string MinSimilarityConfig = "min.similarity";

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(VectorStoreSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(CollectionNameConfig, ConfigType.String, Importance.High, "Collection name for the vector store")
        .Define(TopicConfig, ConfigType.String, Importance.High, "Output topic for search results")
        .Define(QueryTopicConfig, ConfigType.String, Importance.High, "Topic containing query vectors")
        .Define(TopKConfig, ConfigType.Int, 5, Importance.Medium, "Maximum number of results per query")
        .Define(MinSimilarityConfig, ConfigType.String, "0.0", Importance.Medium, "Minimum cosine similarity threshold");

    private string _collectionName = "";
    private string _topic = "";
    private string _queryTopic = "";
    private int _topK = 5;
    private string _minSimilarity = "0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _collectionName = config.TryGetValue(CollectionNameConfig, out var collection)
            ? collection
            : throw new ArgumentException($"Missing required config: {CollectionNameConfig}");

        _topic = config.TryGetValue(TopicConfig, out var topic)
            ? topic
            : throw new ArgumentException($"Missing required config: {TopicConfig}");

        _queryTopic = config.TryGetValue(QueryTopicConfig, out var queryTopic)
            ? queryTopic
            : throw new ArgumentException($"Missing required config: {QueryTopicConfig}");

        if (config.TryGetValue(TopKConfig, out var topK) && int.TryParse(topK, out var topKValue))
        {
            _topK = topKValue;
        }

        if (config.TryGetValue(MinSimilarityConfig, out var minSimilarity))
        {
            _minSimilarity = minSimilarity;
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
                [TopicConfig] = _topic,
                [QueryTopicConfig] = _queryTopic,
                [TopKConfig] = _topK.ToString(),
                [MinSimilarityConfig] = _minSimilarity
            }
        ];
    }
}
