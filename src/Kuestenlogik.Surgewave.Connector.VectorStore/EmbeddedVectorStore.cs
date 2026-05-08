using System.Collections.Concurrent;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Kuestenlogik.Surgewave.Connector.VectorStore;

/// <summary>
/// Lightweight in-memory vector store with SIMD-accelerated cosine similarity search.
/// Vectors are normalized on insert for faster cosine computation.
/// </summary>
public sealed class EmbeddedVectorStore
{
    private readonly ConcurrentDictionary<string, VectorEntry> _entries = new(StringComparer.Ordinal);

    /// <summary>
    /// Gets the number of entries in the store.
    /// </summary>
    public int Count => _entries.Count;

    /// <summary>
    /// Inserts or updates a vector entry. The embedding is normalized before storage.
    /// </summary>
    public void Upsert(VectorEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var normalized = NormalizeVector(entry.Embedding);
        var normalizedEntry = entry with { Embedding = normalized };
        _entries[entry.Id] = normalizedEntry;
    }

    /// <summary>
    /// Searches for the top-K most similar entries to the given query vector.
    /// </summary>
    /// <param name="query">The query embedding vector.</param>
    /// <param name="topK">Maximum number of results to return.</param>
    /// <param name="minSimilarity">Minimum cosine similarity threshold (0.0 to 1.0).</param>
    /// <returns>List of matching entries with their similarity scores, ordered by descending score.</returns>
    public List<(VectorEntry Entry, float Score)> Search(float[] query, int topK, float minSimilarity = 0f)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentOutOfRangeException.ThrowIfNegative(topK);

        var normalizedQuery = NormalizeVector(query);

        // Use a sorted list to maintain top-K efficiently
        var results = new List<(VectorEntry Entry, float Score)>();

        foreach (var kvp in _entries)
        {
            var score = CosineSimilarityNormalized(normalizedQuery, kvp.Value.Embedding);
            if (score >= minSimilarity)
            {
                results.Add((kvp.Value, score));
            }
        }

        results.Sort((a, b) => b.Score.CompareTo(a.Score));

        if (results.Count > topK)
        {
            results.RemoveRange(topK, results.Count - topK);
        }

        return results;
    }

    /// <summary>
    /// Deletes a vector entry by its ID.
    /// </summary>
    /// <returns>True if the entry was found and removed; otherwise false.</returns>
    public bool Delete(string id)
    {
        return _entries.TryRemove(id, out _);
    }

    /// <summary>
    /// Removes all entries from the store.
    /// </summary>
    public void Clear()
    {
        _entries.Clear();
    }

    /// <summary>
    /// Gets all entries in the store (snapshot).
    /// </summary>
    public IReadOnlyCollection<VectorEntry> GetAll()
    {
        return _entries.Values.ToArray();
    }

    /// <summary>
    /// Tries to get a specific entry by ID.
    /// </summary>
    public bool TryGet(string id, out VectorEntry? entry)
    {
        return _entries.TryGetValue(id, out entry);
    }

    /// <summary>
    /// Computes cosine similarity between two already-normalized vectors using SIMD.
    /// For normalized vectors, cosine similarity equals the dot product.
    /// </summary>
    private static float CosineSimilarityNormalized(float[] a, float[] b)
    {
        if (a.Length != b.Length)
        {
            return 0f;
        }

        return DotProductSimd(a, b);
    }

    /// <summary>
    /// SIMD-accelerated dot product using Vector&lt;float&gt;.
    /// </summary>
    private static float DotProductSimd(float[] a, float[] b)
    {
        var spanA = a.AsSpan();
        var spanB = b.AsSpan();
        var sum = 0f;
        var i = 0;

        var simdLength = Vector<float>.Count;
        var vectorizableLength = a.Length - (a.Length % simdLength);

        var refA = MemoryMarshal.Cast<float, byte>(spanA);
        var refB = MemoryMarshal.Cast<float, byte>(spanB);

        // Process SIMD-width chunks
        while (i < vectorizableLength)
        {
            var va = new Vector<float>(spanA[i..]);
            var vb = new Vector<float>(spanB[i..]);
            sum += Vector.Dot(va, vb);
            i += simdLength;
        }

        // Process remaining elements
        for (; i < a.Length; i++)
        {
            sum += a[i] * b[i];
        }

        return sum;
    }

    /// <summary>
    /// Normalizes a vector to unit length (L2 norm).
    /// </summary>
    private static float[] NormalizeVector(float[] vector)
    {
        var magnitude = 0f;
        for (var i = 0; i < vector.Length; i++)
        {
            magnitude += vector[i] * vector[i];
        }

        magnitude = MathF.Sqrt(magnitude);

        if (magnitude < float.Epsilon)
        {
            return vector;
        }

        var result = new float[vector.Length];
        var inverseMagnitude = 1f / magnitude;
        for (var i = 0; i < vector.Length; i++)
        {
            result[i] = vector[i] * inverseMagnitude;
        }

        return result;
    }
}
