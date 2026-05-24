using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Connector.VectorStore;

/// <summary>
/// Static registry for shared vector store instances.
/// Allows sink and source connectors operating on the same collection to share a store.
/// </summary>
internal static class VectorStoreRegistry
{
    private static readonly ConcurrentDictionary<string, EmbeddedVectorStore> Stores = new(StringComparer.Ordinal);

    /// <summary>
    /// Gets an existing store or creates a new one for the given collection name.
    /// </summary>
    public static EmbeddedVectorStore GetOrCreate(string name)
    {
        return Stores.GetOrAdd(name, _ => new EmbeddedVectorStore());
    }

    /// <summary>
    /// Removes a store by collection name.
    /// </summary>
    public static void Remove(string name)
    {
        Stores.TryRemove(name, out _);
    }

    /// <summary>
    /// Clears all registered stores. Used primarily for testing.
    /// </summary>
    internal static void Clear()
    {
        Stores.Clear();
    }
}
