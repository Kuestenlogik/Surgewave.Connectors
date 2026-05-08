using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.VectorStore;

namespace Kuestenlogik.Surgewave.Connector.VectorStore.Tests;

/// <summary>
/// Additional tests for vector operations, similarity search edge cases,
/// connector configuration, and sink task parsing.
/// </summary>
public sealed class VectorStoreAdditionalTests : IDisposable
{
    public VectorStoreAdditionalTests()
    {
        VectorStoreRegistry.Clear();
    }

    public void Dispose()
    {
        VectorStoreRegistry.Clear();
        GC.SuppressFinalize(this);
    }

    // ── Normalization ──

    [Fact]
    public void Upsert_NormalizesEmbeddingToUnitLength()
    {
        var store = new EmbeddedVectorStore();
        // Insert a non-unit-length vector
        store.Upsert(new VectorEntry
        {
            Id = "doc-norm",
            Embedding = [3.0f, 4.0f],   // magnitude = 5
            Timestamp = DateTimeOffset.UtcNow
        });

        // Search with the same un-normalized vector; score should be ~1
        var results = store.Search([3.0f, 4.0f], topK: 1);

        Assert.Single(results);
        Assert.True(results[0].Score > 0.99f, $"Normalized self-similarity should be ~1 but was {results[0].Score}");
    }

    [Fact]
    public void Upsert_ZeroVector_DoesNotNormalize()
    {
        var store = new EmbeddedVectorStore();
        // Zero vector can't be normalized; store should not throw
        store.Upsert(new VectorEntry
        {
            Id = "zero",
            Embedding = [0.0f, 0.0f, 0.0f],
            Timestamp = DateTimeOffset.UtcNow
        });

        Assert.Equal(1, store.Count);
    }

    // ── Similarity search edge cases ──

    [Fact]
    public void Search_OrthogonalVectors_ScoreIsZero()
    {
        var store = new EmbeddedVectorStore();
        store.Upsert(new VectorEntry { Id = "x", Embedding = [1.0f, 0.0f], Timestamp = DateTimeOffset.UtcNow });
        store.Upsert(new VectorEntry { Id = "y", Embedding = [0.0f, 1.0f], Timestamp = DateTimeOffset.UtcNow });

        // Query aligned with x; y is orthogonal so score should be 0
        var results = store.Search([1.0f, 0.0f], topK: 2, minSimilarity: 0f);

        var xResult = results.FirstOrDefault(r => r.Entry.Id == "x");
        var yResult = results.FirstOrDefault(r => r.Entry.Id == "y");

        Assert.NotNull(xResult.Entry);
        Assert.NotNull(yResult.Entry);
        Assert.True(xResult.Score > 0.99f);
        Assert.True(Math.Abs(yResult.Score) < 1e-5f, $"Orthogonal score should be ~0 but was {yResult.Score}");
    }

    [Fact]
    public void Search_HighMinSimilarity_FiltersAll()
    {
        var store = new EmbeddedVectorStore();
        store.Upsert(new VectorEntry { Id = "a", Embedding = [1.0f, 0.0f], Timestamp = DateTimeOffset.UtcNow });

        // minSimilarity above even an identical vector's score?  Use 2.0 to guarantee no match
        var results = store.Search([0.0f, 1.0f], topK: 10, minSimilarity: 0.99f);

        Assert.Empty(results);
    }

    [Fact]
    public void Search_LargeVectors_Works()
    {
        var store = new EmbeddedVectorStore();
        var dim = 512;
        var embedding = new float[dim];
        embedding[0] = 1.0f;  // unit vector in first dimension

        store.Upsert(new VectorEntry
        {
            Id = "large",
            Embedding = embedding,
            Timestamp = DateTimeOffset.UtcNow
        });

        var query = new float[dim];
        query[0] = 1.0f;

        var results = store.Search(query, topK: 1);

        Assert.Single(results);
        Assert.True(results[0].Score > 0.99f);
    }

    [Fact]
    public void Search_ReturnsAllWhenTopKExceedsCount()
    {
        var store = new EmbeddedVectorStore();
        store.Upsert(new VectorEntry { Id = "a", Embedding = [1.0f, 0.0f], Timestamp = DateTimeOffset.UtcNow });
        store.Upsert(new VectorEntry { Id = "b", Embedding = [0.0f, 1.0f], Timestamp = DateTimeOffset.UtcNow });

        var results = store.Search([1.0f, 0.0f], topK: 100);

        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void Search_ScoresAreNonNegativeForValidInputs()
    {
        var store = new EmbeddedVectorStore();
        for (var i = 0; i < 5; i++)
        {
            var emb = new float[4];
            emb[i % 4] = 1.0f;
            store.Upsert(new VectorEntry { Id = $"v{i}", Embedding = emb, Timestamp = DateTimeOffset.UtcNow });
        }

        var results = store.Search([1.0f, 0.0f, 0.0f, 0.0f], topK: 10, minSimilarity: 0f);

        Assert.All(results, r => Assert.True(r.Score >= 0f));
    }

    // ── Connector Start validation ──

    [Fact]
    public void VectorStoreSinkConnector_Start_ThrowsOnMissingCollection()
    {
        using var connector = new VectorStoreSinkConnector();
        connector.Initialize(new ConnectorContext { RequestTaskReconfiguration = () => { }, RaiseError = _ => { } });

        var config = new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.TopicsConfig] = "some-topic"
            // missing collection.name
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void VectorStoreSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new VectorStoreSinkConnector();
        connector.Initialize(new ConnectorContext { RequestTaskReconfiguration = () => { }, RaiseError = _ => { } });

        var config = new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "my-collection"
            // missing topics
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void VectorStoreSourceConnector_Start_ThrowsOnMissingCollection()
    {
        using var connector = new VectorStoreSourceConnector();
        connector.Initialize(new ConnectorContext { RequestTaskReconfiguration = () => { }, RaiseError = _ => { } });

        var config = new Dictionary<string, string>
        {
            [VectorStoreSourceConnector.TopicConfig] = "results",
            [VectorStoreSourceConnector.QueryTopicConfig] = "queries"
            // missing collection.name
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void VectorStoreSourceConnector_Start_ThrowsOnMissingOutputTopic()
    {
        using var connector = new VectorStoreSourceConnector();
        connector.Initialize(new ConnectorContext { RequestTaskReconfiguration = () => { }, RaiseError = _ => { } });

        var config = new Dictionary<string, string>
        {
            [VectorStoreSourceConnector.CollectionNameConfig] = "col",
            [VectorStoreSourceConnector.QueryTopicConfig] = "queries"
            // missing topic
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void VectorStoreSourceConnector_Start_ThrowsOnMissingQueryTopic()
    {
        using var connector = new VectorStoreSourceConnector();
        connector.Initialize(new ConnectorContext { RequestTaskReconfiguration = () => { }, RaiseError = _ => { } });

        var config = new Dictionary<string, string>
        {
            [VectorStoreSourceConnector.CollectionNameConfig] = "col",
            [VectorStoreSourceConnector.TopicConfig] = "results"
            // missing query.topic
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void VectorStoreSinkConnector_TaskConfigs_ContainsAllKeys()
    {
        using var connector = new VectorStoreSinkConnector();
        connector.Initialize(new ConnectorContext { RequestTaskReconfiguration = () => { }, RaiseError = _ => { } });
        connector.Start(new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "col",
            [VectorStoreSinkConnector.TopicsConfig] = "vectors"
        });

        var taskConfigs = connector.TaskConfigs(1);

        Assert.Single(taskConfigs);
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSinkConnector.CollectionNameConfig));
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSinkConnector.TopicsConfig));
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSinkConnector.EmbeddingFieldConfig));
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSinkConnector.ContentFieldConfig));
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSinkConnector.IdFieldConfig));
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSinkConnector.PersistenceTopicConfig));
    }

    [Fact]
    public void VectorStoreSourceConnector_TaskConfigs_ContainsAllKeys()
    {
        using var connector = new VectorStoreSourceConnector();
        connector.Initialize(new ConnectorContext { RequestTaskReconfiguration = () => { }, RaiseError = _ => { } });
        connector.Start(new Dictionary<string, string>
        {
            [VectorStoreSourceConnector.CollectionNameConfig] = "col",
            [VectorStoreSourceConnector.TopicConfig] = "results",
            [VectorStoreSourceConnector.QueryTopicConfig] = "queries"
        });

        var taskConfigs = connector.TaskConfigs(1);

        Assert.Single(taskConfigs);
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSourceConnector.CollectionNameConfig));
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSourceConnector.TopicConfig));
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSourceConnector.QueryTopicConfig));
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSourceConnector.TopKConfig));
        Assert.True(taskConfigs[0].ContainsKey(VectorStoreSourceConnector.MinSimilarityConfig));
    }

    // ── Sink task record parsing ──

    [Fact]
    public async Task SinkTask_RecordWithoutIdField_FallsBackToKey()
    {
        var store = VectorStoreRegistry.GetOrCreate("fallback-col");
        var task = new VectorStoreSinkTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "fallback-col"
        });

        var json = JsonSerializer.SerializeToUtf8Bytes(new
        {
            embedding = new[] { 1.0f, 0.0f }
            // no "id" field
        });

        await task.PutAsync([new SinkRecord
        {
            Topic = "v",
            Partition = 0,
            Offset = 0,
            Key = Encoding.UTF8.GetBytes("key-as-id"),
            Value = json
        }], CancellationToken.None);

        Assert.Equal(1, store.Count);
        Assert.True(store.TryGet("key-as-id", out _));
    }

    [Fact]
    public async Task SinkTask_RecordWithNoIdAndNoKey_IsSkipped()
    {
        var store = VectorStoreRegistry.GetOrCreate("no-id-col");
        var task = new VectorStoreSinkTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "no-id-col"
        });

        var json = JsonSerializer.SerializeToUtf8Bytes(new
        {
            embedding = new[] { 1.0f, 0.0f }
        });

        await task.PutAsync([new SinkRecord
        {
            Topic = "v",
            Partition = 0,
            Offset = 0,
            Key = null,   // no key
            Value = json
        }], CancellationToken.None);

        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task SinkTask_InvalidJsonPayload_IsSkipped()
    {
        var store = VectorStoreRegistry.GetOrCreate("bad-json-col");
        var task = new VectorStoreSinkTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "bad-json-col"
        });

        var badJson = Encoding.UTF8.GetBytes("not json at all }{");

        await task.PutAsync([new SinkRecord
        {
            Topic = "v",
            Partition = 0,
            Offset = 0,
            Key = Encoding.UTF8.GetBytes("k1"),
            Value = badJson
        }], CancellationToken.None);

        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task SinkTask_MissingEmbeddingField_IsSkipped()
    {
        var store = VectorStoreRegistry.GetOrCreate("no-emb-col");
        var task = new VectorStoreSinkTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "no-emb-col"
        });

        var json = JsonSerializer.SerializeToUtf8Bytes(new
        {
            id = "doc1",
            content = "Some content"
            // no embedding field
        });

        await task.PutAsync([new SinkRecord
        {
            Topic = "v", Partition = 0, Offset = 0,
            Value = json
        }], CancellationToken.None);

        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task SinkTask_EmbeddingNotArray_IsSkipped()
    {
        var store = VectorStoreRegistry.GetOrCreate("bad-emb-col");
        var task = new VectorStoreSinkTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "bad-emb-col"
        });

        var json = Encoding.UTF8.GetBytes("""{"id":"doc1","embedding":"not-an-array"}""");

        await task.PutAsync([new SinkRecord
        {
            Topic = "v", Partition = 0, Offset = 0,
            Key = Encoding.UTF8.GetBytes("doc1"),
            Value = json
        }], CancellationToken.None);

        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task SinkTask_NullValueRecord_DeletesByKey()
    {
        var store = VectorStoreRegistry.GetOrCreate("null-val-col");
        store.Upsert(new VectorEntry { Id = "existing", Embedding = [1.0f], Timestamp = DateTimeOffset.UtcNow });

        var task = new VectorStoreSinkTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "null-val-col"
        });

        await task.PutAsync([new SinkRecord
        {
            Topic = "v", Partition = 0, Offset = 0,
            Key = Encoding.UTF8.GetBytes("existing"),
            Value = null!   // tombstone
        }], CancellationToken.None);

        Assert.Equal(0, store.Count);
    }

    // ── Source task ProcessQuery ──

    [Fact]
    public void SourceTask_ProcessQuery_DirectArray_Works()
    {
        var collectionName = "query-array-col";
        var store = VectorStoreRegistry.GetOrCreate(collectionName);
        store.Upsert(new VectorEntry { Id = "doc", Embedding = [1.0f, 0.0f], Timestamp = DateTimeOffset.UtcNow });

        var task = new VectorStoreSourceTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSourceConnector.CollectionNameConfig] = collectionName,
            [VectorStoreSourceConnector.TopicConfig] = "results",
            [VectorStoreSourceConnector.QueryTopicConfig] = "queries"
        });

        var queryData = Encoding.UTF8.GetBytes("[1.0,0.0]");
        var results = task.ProcessQuery(queryData, Encoding.UTF8.GetBytes("q1"));

        Assert.Single(results);
        var json = Encoding.UTF8.GetString(results[0].Value!);
        Assert.Contains("doc", json);
    }

    [Fact]
    public void SourceTask_ProcessQuery_EmbeddingField_Works()
    {
        var collectionName = "query-emb-col";
        var store = VectorStoreRegistry.GetOrCreate(collectionName);
        store.Upsert(new VectorEntry { Id = "doc2", Embedding = [1.0f, 0.0f], Timestamp = DateTimeOffset.UtcNow });

        var task = new VectorStoreSourceTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSourceConnector.CollectionNameConfig] = collectionName,
            [VectorStoreSourceConnector.TopicConfig] = "results",
            [VectorStoreSourceConnector.QueryTopicConfig] = "queries"
        });

        var queryData = Encoding.UTF8.GetBytes("""{"embedding":[1.0,0.0]}""");
        var results = task.ProcessQuery(queryData, null);

        Assert.Single(results);
        var json = Encoding.UTF8.GetString(results[0].Value!);
        Assert.Contains("doc2", json);
    }

    [Fact]
    public void SourceTask_ProcessQuery_QueryField_Works()
    {
        var collectionName = "query-field-col";
        var store = VectorStoreRegistry.GetOrCreate(collectionName);
        store.Upsert(new VectorEntry { Id = "doc3", Embedding = [1.0f, 0.0f], Timestamp = DateTimeOffset.UtcNow });

        var task = new VectorStoreSourceTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSourceConnector.CollectionNameConfig] = collectionName,
            [VectorStoreSourceConnector.TopicConfig] = "results",
            [VectorStoreSourceConnector.QueryTopicConfig] = "queries"
        });

        var queryData = Encoding.UTF8.GetBytes("""{"query":[1.0,0.0]}""");
        var results = task.ProcessQuery(queryData, null);

        Assert.Single(results);
    }

    [Fact]
    public void SourceTask_ProcessQuery_InvalidJson_ReturnsEmpty()
    {
        var collectionName = "query-bad-json";
        VectorStoreRegistry.GetOrCreate(collectionName);

        var task = new VectorStoreSourceTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSourceConnector.CollectionNameConfig] = collectionName,
            [VectorStoreSourceConnector.TopicConfig] = "results",
            [VectorStoreSourceConnector.QueryTopicConfig] = "queries"
        });

        var queryData = Encoding.UTF8.GetBytes("this is not json");
        var results = task.ProcessQuery(queryData, null);

        Assert.Empty(results);
    }

    [Fact]
    public void SourceTask_ProcessQuery_EmptyStore_ReturnsResponseWithNoResults()
    {
        var collectionName = "query-empty-store";
        VectorStoreRegistry.GetOrCreate(collectionName);

        var task = new VectorStoreSourceTask();
        task.Initialize(new TaskContext());
        task.Start(new Dictionary<string, string>
        {
            [VectorStoreSourceConnector.CollectionNameConfig] = collectionName,
            [VectorStoreSourceConnector.TopicConfig] = "results",
            [VectorStoreSourceConnector.QueryTopicConfig] = "queries"
        });

        var queryData = Encoding.UTF8.GetBytes("[1.0,0.0]");
        var results = task.ProcessQuery(queryData, Encoding.UTF8.GetBytes("empty-query"));

        // Should still produce a response record, just with empty results
        Assert.Single(results);
        var json = Encoding.UTF8.GetString(results[0].Value!);
        Assert.Contains("\"results\":[]", json);
    }
}
