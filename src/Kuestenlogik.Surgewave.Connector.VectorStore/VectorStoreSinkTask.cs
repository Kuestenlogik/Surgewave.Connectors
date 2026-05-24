using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.VectorStore;

/// <summary>
/// Sink task that parses JSON records and upserts vector entries into the embedded store.
/// Supports tombstone records (null value) for deletion.
/// On flush, optionally persists a snapshot to a compacted Surgewave topic.
/// </summary>
public sealed class VectorStoreSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private EmbeddedVectorStore _store = null!;
    private string _embeddingField = "embedding";
    private string _contentField = "content";
    private string _idField = "id";
    private string _persistenceTopic = "";
    private string _collectionName = "";

    public override void Start(IDictionary<string, string> config)
    {
        _collectionName = config[VectorStoreSinkConnector.CollectionNameConfig];
        _store = VectorStoreRegistry.GetOrCreate(_collectionName);

        if (config.TryGetValue(VectorStoreSinkConnector.EmbeddingFieldConfig, out var embeddingField))
        {
            _embeddingField = embeddingField;
        }

        if (config.TryGetValue(VectorStoreSinkConnector.ContentFieldConfig, out var contentField))
        {
            _contentField = contentField;
        }

        if (config.TryGetValue(VectorStoreSinkConnector.IdFieldConfig, out var idField))
        {
            _idField = idField;
        }

        if (config.TryGetValue(VectorStoreSinkConnector.PersistenceTopicConfig, out var persistenceTopic))
        {
            _persistenceTopic = persistenceTopic;
        }
    }

    public override void Stop()
    {
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Tombstone record (null value) means delete
            if (record.Value is null || record.Value.Length == 0)
            {
                var deleteId = record.Key is not null
                    ? Encoding.UTF8.GetString(record.Key)
                    : null;

                if (deleteId is not null)
                {
                    _store.Delete(deleteId);
                }

                continue;
            }

            var json = ParseJson(record.Value);
            if (json is null)
            {
                continue;
            }

            var id = ExtractId(json, record);
            if (id is null)
            {
                continue;
            }

            var embedding = ExtractEmbedding(json);
            if (embedding is null)
            {
                continue;
            }

            var content = ExtractString(json, _contentField);
            var metadata = ExtractMetadata(json);

            var entry = new VectorEntry
            {
                Id = id,
                Embedding = embedding,
                Content = content,
                Metadata = metadata,
                Timestamp = record.Timestamp != default ? record.Timestamp : DateTimeOffset.UtcNow
            };

            _store.Upsert(entry);
        }

        return Task.CompletedTask;
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(_persistenceTopic) || Context.Producer is null)
        {
            return;
        }

        // Produce a snapshot of all entries to the compacted persistence topic
        var allEntries = _store.GetAll();
        foreach (var entry in allEntries)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var key = Encoding.UTF8.GetBytes(entry.Id);
            var value = JsonSerializer.SerializeToUtf8Bytes(entry, JsonContext.Default.VectorEntry);

            await Context.Producer.ProduceAsync(_persistenceTopic, key, value, cancellationToken);
        }
    }

    private static JsonDocument? ParseJson(byte[] data)
    {
        try
        {
            return JsonDocument.Parse(data);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private string? ExtractId(JsonDocument json, SinkRecord record)
    {
        if (json.RootElement.TryGetProperty(_idField, out var idElement) &&
            idElement.ValueKind == JsonValueKind.String)
        {
            return idElement.GetString();
        }

        // Fall back to record key
        if (record.Key is not null)
        {
            return Encoding.UTF8.GetString(record.Key);
        }

        return null;
    }

    private float[]? ExtractEmbedding(JsonDocument json)
    {
        if (!json.RootElement.TryGetProperty(_embeddingField, out var embeddingElement))
        {
            return null;
        }

        if (embeddingElement.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        var length = embeddingElement.GetArrayLength();
        var embedding = new float[length];
        var index = 0;

        foreach (var item in embeddingElement.EnumerateArray())
        {
            if (item.ValueKind == JsonValueKind.Number)
            {
                embedding[index++] = item.GetSingle();
            }
            else
            {
                return null;
            }
        }

        return embedding;
    }

    private static string? ExtractString(JsonDocument json, string field)
    {
        if (json.RootElement.TryGetProperty(field, out var element) &&
            element.ValueKind == JsonValueKind.String)
        {
            return element.GetString();
        }

        return null;
    }

    private static Dictionary<string, string>? ExtractMetadata(JsonDocument json)
    {
        if (!json.RootElement.TryGetProperty("metadata", out var metadataElement))
        {
            return null;
        }

        if (metadataElement.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var property in metadataElement.EnumerateObject())
        {
            metadata[property.Name] = property.Value.ToString();
        }

        return metadata;
    }
}
