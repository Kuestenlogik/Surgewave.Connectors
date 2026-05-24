using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.VectorStore;

/// <summary>
/// Source task that polls a query topic for incoming query vectors,
/// searches the embedded vector store, and produces results to an output topic.
/// </summary>
public sealed class VectorStoreSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private EmbeddedVectorStore _store = null!;
    private string _topic = "";
    private string _queryTopic = "";
    private int _topK = 5;
    private float _minSimilarity;
    private long _queryIndex;
    private readonly Dictionary<string, object> _sourcePartition = new(StringComparer.Ordinal);

    public override void Start(IDictionary<string, string> config)
    {
        var collectionName = config[VectorStoreSourceConnector.CollectionNameConfig];
        _store = VectorStoreRegistry.GetOrCreate(collectionName);
        _topic = config[VectorStoreSourceConnector.TopicConfig];
        _queryTopic = config[VectorStoreSourceConnector.QueryTopicConfig];

        _sourcePartition["collection"] = collectionName;

        if (config.TryGetValue(VectorStoreSourceConnector.TopKConfig, out var topK) &&
            int.TryParse(topK, out var topKValue))
        {
            _topK = topKValue;
        }

        if (config.TryGetValue(VectorStoreSourceConnector.MinSimilarityConfig, out var minSim) &&
            float.TryParse(minSim, NumberStyles.Float, CultureInfo.InvariantCulture, out var minSimValue))
        {
            _minSimilarity = minSimValue;
        }
    }

    public override void Stop()
    {
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        // In a real implementation, this would consume from the query topic.
        // For the connector framework, the worker handles topic consumption and
        // feeds records through. Here we wait briefly if no queries are pending.
        await Task.Delay(100, cancellationToken);
        return [];
    }

    /// <summary>
    /// Processes a query record and returns search results as source records.
    /// Called by the connector framework when records arrive on the query topic.
    /// </summary>
    internal IReadOnlyList<SourceRecord> ProcessQuery(byte[] queryData, byte[]? queryKey)
    {
        var queryVector = ParseQueryVector(queryData);
        if (queryVector is null)
        {
            return [];
        }

        var queryId = queryKey is not null
            ? Encoding.UTF8.GetString(queryKey)
            : Guid.NewGuid().ToString("N");

        var results = _store.Search(queryVector, _topK, _minSimilarity);

        var resultItems = new List<SearchResultItem>(results.Count);
        foreach (var (entry, score) in results)
        {
            resultItems.Add(new SearchResultItem
            {
                Id = entry.Id,
                Score = score,
                Content = entry.Content,
                Metadata = entry.Metadata
            });
        }

        var response = new SearchResponse
        {
            QueryId = queryId,
            Results = resultItems
        };

        var responseJson = JsonSerializer.SerializeToUtf8Bytes(response, JsonContext.Default.SearchResponse);

        var sourceOffset = new Dictionary<string, object>
        {
            ["query_index"] = Interlocked.Increment(ref _queryIndex)
        };

        return
        [
            new SourceRecord
            {
                SourcePartition = _sourcePartition,
                SourceOffset = sourceOffset,
                Topic = _topic,
                Key = queryKey,
                Value = responseJson
            }
        ];
    }

    private static float[]? ParseQueryVector(byte[] data)
    {
        try
        {
            var doc = JsonDocument.Parse(data);

            // Try to parse as { "embedding": [...] } or directly as [...]
            JsonElement arrayElement;

            if (doc.RootElement.ValueKind == JsonValueKind.Array)
            {
                arrayElement = doc.RootElement;
            }
            else if (doc.RootElement.TryGetProperty("embedding", out var embeddingElement) &&
                     embeddingElement.ValueKind == JsonValueKind.Array)
            {
                arrayElement = embeddingElement;
            }
            else if (doc.RootElement.TryGetProperty("query", out var queryElement) &&
                     queryElement.ValueKind == JsonValueKind.Array)
            {
                arrayElement = queryElement;
            }
            else
            {
                return null;
            }

            var length = arrayElement.GetArrayLength();
            var vector = new float[length];
            var index = 0;

            foreach (var item in arrayElement.EnumerateArray())
            {
                if (item.ValueKind == JsonValueKind.Number)
                {
                    vector[index++] = item.GetSingle();
                }
                else
                {
                    return null;
                }
            }

            return vector;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}

/// <summary>
/// JSON response model for search results.
/// </summary>
internal sealed class SearchResponse
{
    public required string QueryId { get; init; }
    public required List<SearchResultItem> Results { get; init; }
}

/// <summary>
/// A single search result item.
/// </summary>
internal sealed class SearchResultItem
{
    public required string Id { get; init; }
    public required float Score { get; init; }
    public string? Content { get; init; }
    public Dictionary<string, string>? Metadata { get; init; }
}
