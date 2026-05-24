namespace Kuestenlogik.Surgewave.Connector.TextChunking;

/// <summary>
/// Interface for text chunking strategies.
/// </summary>
public interface ITextChunker
{
    /// <summary>
    /// Splits the input text into chunks.
    /// </summary>
    /// <param name="text">The text to split.</param>
    /// <returns>An enumerable of text chunks with metadata.</returns>
    IEnumerable<TextChunk> Chunk(string text);
}

/// <summary>
/// Represents a chunk of text with associated metadata.
/// </summary>
public sealed record TextChunk
{
    /// <summary>
    /// The chunk text content.
    /// </summary>
    public required string Text { get; init; }

    /// <summary>
    /// Zero-based index of this chunk in the sequence.
    /// </summary>
    public int Index { get; init; }

    /// <summary>
    /// Character offset in the original text where this chunk starts.
    /// </summary>
    public int StartOffset { get; init; }

    /// <summary>
    /// Character offset in the original text where this chunk ends (exclusive).
    /// </summary>
    public int EndOffset { get; init; }

    /// <summary>
    /// Total number of chunks produced from the original text.
    /// </summary>
    public int TotalChunks { get; init; }
}
