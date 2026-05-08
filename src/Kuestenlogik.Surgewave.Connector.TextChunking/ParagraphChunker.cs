namespace Kuestenlogik.Surgewave.Connector.TextChunking;

/// <summary>
/// Splits text into chunks based on paragraph boundaries.
/// </summary>
public sealed class ParagraphChunker : ITextChunker
{
    private readonly int _maxChunkSize;
    private readonly int _overlap;
    private readonly string _separator;
    private readonly bool _trimWhitespace;
    private readonly int _minChunkSize;

    public ParagraphChunker(int maxChunkSize, int overlap, string separator = "\n\n", bool trimWhitespace = true, int minChunkSize = 1)
    {
        if (maxChunkSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxChunkSize), "Max chunk size must be positive");
        if (overlap < 0)
            throw new ArgumentOutOfRangeException(nameof(overlap), "Overlap cannot be negative");

        _maxChunkSize = maxChunkSize;
        _overlap = overlap;
        _separator = separator;
        _trimWhitespace = trimWhitespace;
        _minChunkSize = minChunkSize;
    }

    public IEnumerable<TextChunk> Chunk(string text)
    {
        if (string.IsNullOrEmpty(text))
            yield break;

        var paragraphs = SplitIntoParagraphs(text);
        if (paragraphs.Count == 0)
            yield break;

        var chunks = new List<TextChunk>();
        var currentParagraphs = new List<(string Text, int StartOffset, int EndOffset)>();
        var currentLength = 0;
        var chunkIndex = 0;

        foreach (var paragraph in paragraphs)
        {
            var paragraphLength = paragraph.Text.Length;

            // If adding this paragraph would exceed the max size, finalize current chunk
            if (currentLength + paragraphLength > _maxChunkSize && currentParagraphs.Count > 0)
            {
                chunks.Add(CreateChunk(currentParagraphs, chunkIndex++));

                // Apply overlap: keep some paragraphs for the next chunk
                currentParagraphs = ApplyOverlap(currentParagraphs);
                currentLength = currentParagraphs.Sum(p => p.Text.Length);
            }

            currentParagraphs.Add(paragraph);
            currentLength += paragraphLength;
        }

        // Finalize the last chunk
        if (currentParagraphs.Count > 0)
        {
            chunks.Add(CreateChunk(currentParagraphs, chunkIndex));
        }

        var totalChunks = chunks.Count;
        foreach (var chunk in chunks)
        {
            yield return chunk with { TotalChunks = totalChunks };
        }
    }

    private TextChunk CreateChunk(List<(string Text, int StartOffset, int EndOffset)> paragraphs, int index)
    {
        var text = string.Join(_separator, paragraphs.Select(p => p.Text));
        if (_trimWhitespace)
            text = text.Trim();

        return new TextChunk
        {
            Text = text,
            Index = index,
            StartOffset = paragraphs.First().StartOffset,
            EndOffset = paragraphs.Last().EndOffset,
            TotalChunks = 0
        };
    }

    private List<(string Text, int StartOffset, int EndOffset)> ApplyOverlap(List<(string Text, int StartOffset, int EndOffset)> paragraphs)
    {
        if (_overlap == 0 || paragraphs.Count <= 1)
            return [];

        // Calculate how many paragraphs to keep based on overlap
        var totalLength = paragraphs.Sum(p => p.Text.Length);
        var targetLength = Math.Min(_overlap, totalLength);
        var result = new List<(string Text, int StartOffset, int EndOffset)>();
        var runningLength = 0;

        // Work backwards to get paragraphs that fit in overlap
        for (var i = paragraphs.Count - 1; i >= 0 && runningLength < targetLength; i--)
        {
            result.Insert(0, paragraphs[i]);
            runningLength += paragraphs[i].Text.Length;
        }

        return result;
    }

    private List<(string Text, int StartOffset, int EndOffset)> SplitIntoParagraphs(string text)
    {
        var paragraphs = new List<(string, int, int)>();
        var parts = text.Split([_separator], StringSplitOptions.None);
        var currentOffset = 0;

        foreach (var part in parts)
        {
            var trimmed = _trimWhitespace ? part.Trim() : part;

            if (trimmed.Length >= _minChunkSize)
            {
                paragraphs.Add((trimmed, currentOffset, currentOffset + part.Length));
            }

            currentOffset += part.Length + _separator.Length;
        }

        return paragraphs;
    }
}
