using Kuestenlogik.Surgewave.Connector.VectorStore;

namespace Kuestenlogik.Surgewave.Connector.VectorStore.Tests;

/// <summary>
/// Extended tests for EmbeddedVectorStore operations.
/// </summary>
public sealed class EmbeddedVectorStoreExtendedTests
{
    [Fact]
    public void Upsert_OverwritesExistingEntry()
    {
        var store = new EmbeddedVectorStore();

        store.Upsert(new VectorEntry
        {
            Id = "doc1",
            Embedding = [1.0f, 0.0f, 0.0f],
            Content = "Original",
            Timestamp = DateTimeOffset.UtcNow
        });

        store.Upsert(new VectorEntry
        {
            Id = "doc1",
            Embedding = [0.0f, 1.0f, 0.0f],
            Content = "Updated",
            Timestamp = DateTimeOffset.UtcNow
        });

        Assert.Equal(1, store.Count);
        Assert.True(store.TryGet("doc1", out var entry));
        Assert.Equal("Updated", entry!.Content);
    }

    [Fact]
    public void Upsert_NullEntry_ThrowsArgumentNull()
    {
        var store = new EmbeddedVectorStore();

        Assert.Throws<ArgumentNullException>(() => store.Upsert(null!));
    }

    [Fact]
    public void Search_NullQuery_ThrowsArgumentNull()
    {
        var store = new EmbeddedVectorStore();

        Assert.Throws<ArgumentNullException>(() => store.Search(null!, 5));
    }

    [Fact]
    public void Search_NegativeTopK_ThrowsArgumentOutOfRange()
    {
        var store = new EmbeddedVectorStore();

        Assert.Throws<ArgumentOutOfRangeException>(() => store.Search([1.0f], -1));
    }

    [Fact]
    public void Search_EmptyStore_ReturnsEmpty()
    {
        var store = new EmbeddedVectorStore();

        var results = store.Search([1.0f, 0.0f], topK: 5);

        Assert.Empty(results);
    }

    [Fact]
    public void Search_TopKZero_ReturnsEmpty()
    {
        var store = new EmbeddedVectorStore();
        store.Upsert(new VectorEntry
        {
            Id = "doc1",
            Embedding = [1.0f, 0.0f],
            Timestamp = DateTimeOffset.UtcNow
        });

        var results = store.Search([1.0f, 0.0f], topK: 0);

        Assert.Empty(results);
    }

    [Fact]
    public void Search_ResultsOrderedByDescendingScore()
    {
        var store = new EmbeddedVectorStore();

        store.Upsert(new VectorEntry
        {
            Id = "far",
            Embedding = [0.0f, 1.0f, 0.0f],
            Timestamp = DateTimeOffset.UtcNow
        });

        store.Upsert(new VectorEntry
        {
            Id = "close",
            Embedding = [0.9f, 0.1f, 0.0f],
            Timestamp = DateTimeOffset.UtcNow
        });

        store.Upsert(new VectorEntry
        {
            Id = "exact",
            Embedding = [1.0f, 0.0f, 0.0f],
            Timestamp = DateTimeOffset.UtcNow
        });

        var results = store.Search([1.0f, 0.0f, 0.0f], topK: 3);

        Assert.Equal(3, results.Count);
        Assert.Equal("exact", results[0].Entry.Id);
        Assert.True(results[0].Score >= results[1].Score);
        Assert.True(results[1].Score >= results[2].Score);
    }

    [Fact]
    public void Clear_RemovesAllEntries()
    {
        var store = new EmbeddedVectorStore();

        for (var i = 0; i < 5; i++)
        {
            store.Upsert(new VectorEntry
            {
                Id = $"doc{i}",
                Embedding = [1.0f, 0.0f],
                Timestamp = DateTimeOffset.UtcNow
            });
        }

        Assert.Equal(5, store.Count);
        store.Clear();
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void GetAll_ReturnsAllEntries()
    {
        var store = new EmbeddedVectorStore();

        store.Upsert(new VectorEntry { Id = "a", Embedding = [1.0f], Timestamp = DateTimeOffset.UtcNow });
        store.Upsert(new VectorEntry { Id = "b", Embedding = [0.5f], Timestamp = DateTimeOffset.UtcNow });
        store.Upsert(new VectorEntry { Id = "c", Embedding = [0.2f], Timestamp = DateTimeOffset.UtcNow });

        var all = store.GetAll();

        Assert.Equal(3, all.Count);
    }

    [Fact]
    public void TryGet_ExistingEntry_ReturnsTrue()
    {
        var store = new EmbeddedVectorStore();
        store.Upsert(new VectorEntry
        {
            Id = "exists",
            Embedding = [1.0f, 0.5f],
            Content = "Found me",
            Timestamp = DateTimeOffset.UtcNow
        });

        Assert.True(store.TryGet("exists", out var entry));
        Assert.NotNull(entry);
        Assert.Equal("Found me", entry.Content);
    }

    [Fact]
    public void TryGet_NonExistent_ReturnsFalse()
    {
        var store = new EmbeddedVectorStore();

        Assert.False(store.TryGet("ghost", out var entry));
        Assert.Null(entry);
    }

    [Fact]
    public void Delete_NonExistent_ReturnsFalse()
    {
        var store = new EmbeddedVectorStore();

        Assert.False(store.Delete("nope"));
    }

    [Fact]
    public void Search_DifferentDimensions_ReturnsZeroScore()
    {
        var store = new EmbeddedVectorStore();
        store.Upsert(new VectorEntry
        {
            Id = "3d",
            Embedding = [1.0f, 0.0f, 0.0f],
            Timestamp = DateTimeOffset.UtcNow
        });

        // Query with different dimension - cosine similarity returns 0 for mismatched lengths
        var results = store.Search([1.0f, 0.0f], topK: 1);

        // 0.0 score still passes default minSimilarity of 0f, so it appears in results
        Assert.Single(results);
        Assert.Equal(0f, results[0].Score);
    }

    [Fact]
    public void VectorEntry_WithMetadata_IsPreserved()
    {
        var store = new EmbeddedVectorStore();
        var metadata = new Dictionary<string, string>
        {
            ["source"] = "unit-test",
            ["category"] = "documentation"
        };

        store.Upsert(new VectorEntry
        {
            Id = "meta-entry",
            Embedding = [0.5f, 0.5f],
            Content = "Test content",
            Metadata = metadata,
            Timestamp = DateTimeOffset.UtcNow
        });

        Assert.True(store.TryGet("meta-entry", out var entry));
        Assert.NotNull(entry!.Metadata);
        Assert.Equal("unit-test", entry.Metadata["source"]);
        Assert.Equal("documentation", entry.Metadata["category"]);
    }
}
