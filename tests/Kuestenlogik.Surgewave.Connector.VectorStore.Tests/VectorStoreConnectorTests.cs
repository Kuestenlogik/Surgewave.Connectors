using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.VectorStore;

namespace Kuestenlogik.Surgewave.Connector.VectorStore.Tests;

public class VectorStoreConnectorTests : IDisposable
{
    public VectorStoreConnectorTests()
    {
        // Ensure clean state for each test
        VectorStoreRegistry.Clear();
    }

    public void Dispose()
    {
        VectorStoreRegistry.Clear();
        GC.SuppressFinalize(this);
    }

    [Fact]
    public void EmbeddedVectorStore_Upsert_And_Search()
    {
        var store = new EmbeddedVectorStore();

        store.Upsert(new VectorEntry
        {
            Id = "doc1",
            Embedding = [1.0f, 0.0f, 0.0f],
            Content = "Hello world",
            Timestamp = DateTimeOffset.UtcNow
        });

        store.Upsert(new VectorEntry
        {
            Id = "doc2",
            Embedding = [0.0f, 1.0f, 0.0f],
            Content = "Goodbye world",
            Timestamp = DateTimeOffset.UtcNow
        });

        store.Upsert(new VectorEntry
        {
            Id = "doc3",
            Embedding = [0.9f, 0.1f, 0.0f],
            Content = "Similar to doc1",
            Timestamp = DateTimeOffset.UtcNow
        });

        // Query with vector close to doc1
        var results = store.Search([1.0f, 0.0f, 0.0f], topK: 2);

        Assert.Equal(2, results.Count);
        Assert.Equal("doc1", results[0].Entry.Id);
        Assert.True(results[0].Score > results[1].Score, "First result should have higher similarity score");
    }

    [Fact]
    public void EmbeddedVectorStore_Delete_RemovesEntry()
    {
        var store = new EmbeddedVectorStore();

        store.Upsert(new VectorEntry
        {
            Id = "doc1",
            Embedding = [1.0f, 0.0f, 0.0f],
            Content = "Test",
            Timestamp = DateTimeOffset.UtcNow
        });

        Assert.Equal(1, store.Count);

        var deleted = store.Delete("doc1");

        Assert.True(deleted);
        Assert.Equal(0, store.Count);

        // Deleting again returns false
        var deletedAgain = store.Delete("doc1");
        Assert.False(deletedAgain);
    }

    [Fact]
    public void EmbeddedVectorStore_CosineSimilarity_Correct()
    {
        var store = new EmbeddedVectorStore();

        store.Upsert(new VectorEntry
        {
            Id = "identity",
            Embedding = [1.0f, 0.0f, 0.0f],
            Content = "Identity vector",
            Timestamp = DateTimeOffset.UtcNow
        });

        // Search with the same vector should yield score very close to 1.0
        var results = store.Search([1.0f, 0.0f, 0.0f], topK: 1);

        Assert.Single(results);
        Assert.True(results[0].Score > 0.99f, $"Identity vector score should be ~1.0 but was {results[0].Score}");
    }

    [Fact]
    public void EmbeddedVectorStore_TopK_LimitsResults()
    {
        var store = new EmbeddedVectorStore();

        for (var i = 0; i < 10; i++)
        {
            var embedding = new float[3];
            embedding[i % 3] = 1.0f;
            store.Upsert(new VectorEntry
            {
                Id = $"doc{i}",
                Embedding = embedding,
                Content = $"Document {i}",
                Timestamp = DateTimeOffset.UtcNow
            });
        }

        var results = store.Search([1.0f, 0.0f, 0.0f], topK: 3);

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public void EmbeddedVectorStore_MinSimilarity_FiltersResults()
    {
        var store = new EmbeddedVectorStore();

        store.Upsert(new VectorEntry
        {
            Id = "close",
            Embedding = [1.0f, 0.1f, 0.0f],
            Content = "Close match",
            Timestamp = DateTimeOffset.UtcNow
        });

        store.Upsert(new VectorEntry
        {
            Id = "orthogonal",
            Embedding = [0.0f, 0.0f, 1.0f],
            Content = "Orthogonal",
            Timestamp = DateTimeOffset.UtcNow
        });

        // High min similarity should filter out the orthogonal vector
        var results = store.Search([1.0f, 0.0f, 0.0f], topK: 10, minSimilarity: 0.5f);

        Assert.Single(results);
        Assert.Equal("close", results[0].Entry.Id);
    }

    [Fact]
    public async Task VectorStoreSinkTask_ParsesJson_And_Upserts()
    {
        var store = VectorStoreRegistry.GetOrCreate("test-collection");
        var task = new VectorStoreSinkTask();
        task.Initialize(new TaskContext());

        var config = new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "test-collection",
            [VectorStoreSinkConnector.EmbeddingFieldConfig] = "embedding",
            [VectorStoreSinkConnector.ContentFieldConfig] = "content",
            [VectorStoreSinkConnector.IdFieldConfig] = "id"
        };
        task.Start(config);

        var jsonPayload = JsonSerializer.SerializeToUtf8Bytes(new
        {
            id = "vec1",
            embedding = new[] { 1.0f, 0.5f, 0.2f },
            content = "Test document",
            metadata = new { source = "unit-test" }
        });

        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "vectors",
                Partition = 0,
                Offset = 0,
                Value = jsonPayload,
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(1, store.Count);
        Assert.True(store.TryGet("vec1", out var entry));
        Assert.Equal("Test document", entry!.Content);
    }

    [Fact]
    public async Task VectorStoreSinkTask_TombstoneRecord_Deletes()
    {
        var store = VectorStoreRegistry.GetOrCreate("tombstone-collection");

        // Pre-populate the store
        store.Upsert(new VectorEntry
        {
            Id = "to-delete",
            Embedding = [1.0f, 0.0f],
            Content = "Will be deleted",
            Timestamp = DateTimeOffset.UtcNow
        });

        Assert.Equal(1, store.Count);

        var task = new VectorStoreSinkTask();
        task.Initialize(new TaskContext());

        var config = new Dictionary<string, string>
        {
            [VectorStoreSinkConnector.CollectionNameConfig] = "tombstone-collection",
            [VectorStoreSinkConnector.EmbeddingFieldConfig] = "embedding",
            [VectorStoreSinkConnector.ContentFieldConfig] = "content",
            [VectorStoreSinkConnector.IdFieldConfig] = "id"
        };
        task.Start(config);

        // Send a tombstone record (empty value) with the key set
        var records = new List<SinkRecord>
        {
            new()
            {
                Topic = "vectors",
                Partition = 0,
                Offset = 1,
                Key = Encoding.UTF8.GetBytes("to-delete"),
                Value = [],
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void VectorStoreRegistry_SharedInstance()
    {
        var store1 = VectorStoreRegistry.GetOrCreate("shared");
        var store2 = VectorStoreRegistry.GetOrCreate("shared");
        var store3 = VectorStoreRegistry.GetOrCreate("different");

        Assert.Same(store1, store2);
        Assert.NotSame(store1, store3);
    }

    [Fact]
    public void VectorStoreSinkConnector_Config_HasRequiredFields()
    {
        var connector = new VectorStoreSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == "collection.name");
        Assert.Contains(connector.Config.Keys, k => k.Name == "topics");
        Assert.Contains(connector.Config.Keys, k => k.Name == "embedding.field");
        Assert.Contains(connector.Config.Keys, k => k.Name == "content.field");
        Assert.Contains(connector.Config.Keys, k => k.Name == "id.field");
        Assert.Contains(connector.Config.Keys, k => k.Name == "persistence.topic");

        // Verify importance levels
        var collectionKey = connector.Config.Keys.First(k => k.Name == "collection.name");
        Assert.Equal(Connect.Configuration.Importance.High, collectionKey.Importance);

        var persistenceKey = connector.Config.Keys.First(k => k.Name == "persistence.topic");
        Assert.Equal(Connect.Configuration.Importance.Low, persistenceKey.Importance);
    }

    [Fact]
    public void VectorStoreSourceConnector_Config_HasRequiredFields()
    {
        var connector = new VectorStoreSourceConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == "collection.name");
        Assert.Contains(connector.Config.Keys, k => k.Name == "topic");
        Assert.Contains(connector.Config.Keys, k => k.Name == "query.topic");
        Assert.Contains(connector.Config.Keys, k => k.Name == "top.k");
        Assert.Contains(connector.Config.Keys, k => k.Name == "min.similarity");

        // Verify importance levels
        var topicKey = connector.Config.Keys.First(k => k.Name == "topic");
        Assert.Equal(Connect.Configuration.Importance.High, topicKey.Importance);

        var topKKey = connector.Config.Keys.First(k => k.Name == "top.k");
        Assert.Equal(Connect.Configuration.Importance.Medium, topKKey.Importance);
    }
}
