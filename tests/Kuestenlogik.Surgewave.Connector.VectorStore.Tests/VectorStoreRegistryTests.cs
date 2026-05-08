using Kuestenlogik.Surgewave.Connector.VectorStore;

namespace Kuestenlogik.Surgewave.Connector.VectorStore.Tests;

/// <summary>
/// Tests for the VectorStoreRegistry shared instance management.
/// </summary>
public sealed class VectorStoreRegistryTests : IDisposable
{
    public VectorStoreRegistryTests()
    {
        VectorStoreRegistry.Clear();
    }

    public void Dispose()
    {
        VectorStoreRegistry.Clear();
        GC.SuppressFinalize(this);
    }

    [Fact]
    public void GetOrCreate_SameName_ReturnsSameInstance()
    {
        var store1 = VectorStoreRegistry.GetOrCreate("shared-collection");
        var store2 = VectorStoreRegistry.GetOrCreate("shared-collection");

        Assert.Same(store1, store2);
    }

    [Fact]
    public void GetOrCreate_DifferentNames_ReturnsDifferentInstances()
    {
        var store1 = VectorStoreRegistry.GetOrCreate("collection-a");
        var store2 = VectorStoreRegistry.GetOrCreate("collection-b");

        Assert.NotSame(store1, store2);
    }

    [Fact]
    public void Remove_ExistingStore_SubsequentGetOrCreateReturnsNew()
    {
        var store1 = VectorStoreRegistry.GetOrCreate("removable");
        store1.Upsert(new VectorEntry
        {
            Id = "doc1",
            Embedding = [1.0f],
            Timestamp = DateTimeOffset.UtcNow
        });
        Assert.Equal(1, store1.Count);

        VectorStoreRegistry.Remove("removable");
        var store2 = VectorStoreRegistry.GetOrCreate("removable");

        Assert.NotSame(store1, store2);
        Assert.Equal(0, store2.Count);
    }

    [Fact]
    public void Remove_NonExistent_DoesNotThrow()
    {
        VectorStoreRegistry.Remove("nonexistent");
    }

    [Fact]
    public void Clear_RemovesAll_SubsequentGetOrCreateReturnsNew()
    {
        var store1 = VectorStoreRegistry.GetOrCreate("a");
        var store2 = VectorStoreRegistry.GetOrCreate("b");

        VectorStoreRegistry.Clear();

        var store3 = VectorStoreRegistry.GetOrCreate("a");
        Assert.NotSame(store1, store3);
    }
}
