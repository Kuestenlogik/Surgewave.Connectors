namespace Kuestenlogik.Surgewave.Connector.VectorStore;

/// <summary>
/// Represents a single vector entry stored in the embedded vector store.
/// </summary>
public sealed record VectorEntry
{
    /// <summary>
    /// Unique identifier for the vector entry.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// The embedding vector (stored normalized for fast cosine similarity).
    /// </summary>
    public required float[] Embedding { get; init; }

    /// <summary>
    /// Optional text content associated with the vector.
    /// </summary>
    public string? Content { get; init; }

    /// <summary>
    /// Optional metadata key-value pairs.
    /// </summary>
    public Dictionary<string, string>? Metadata { get; init; }

    /// <summary>
    /// Timestamp when the entry was created or last updated.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }
}
