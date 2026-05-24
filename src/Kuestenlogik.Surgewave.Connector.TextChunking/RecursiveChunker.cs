namespace Kuestenlogik.Surgewave.Connector.TextChunking;

/// <summary>
/// Splits text recursively using a hierarchy of separators.
/// Similar to LangChain's RecursiveCharacterTextSplitter.
/// </summary>
public sealed class RecursiveChunker : ITextChunker
{
    private readonly int _chunkSize;
    private readonly int _overlap;
    private readonly string[] _separators;
    private readonly bool _trimWhitespace;
    private readonly int _minChunkSize;

    public RecursiveChunker(int chunkSize, int overlap, string[]? separators = null, bool trimWhitespace = true, int minChunkSize = 1)
    {
        if (chunkSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be positive");
        if (overlap < 0)
            throw new ArgumentOutOfRangeException(nameof(overlap), "Overlap cannot be negative");
        if (overlap >= chunkSize)
            throw new ArgumentException("Overlap must be less than chunk size", nameof(overlap));

        _chunkSize = chunkSize;
        _overlap = overlap;
        _separators = separators ?? ["\n\n", "\n", " ", ""];
        _trimWhitespace = trimWhitespace;
        _minChunkSize = minChunkSize;
    }

    public IEnumerable<TextChunk> Chunk(string text)
    {
        if (string.IsNullOrEmpty(text))
            yield break;

        var rawChunks = SplitText(text, 0);
        var chunks = MergeChunksWithOverlap(rawChunks.ToList());

        var totalChunks = chunks.Count;
        for (var i = 0; i < chunks.Count; i++)
        {
            yield return chunks[i] with { Index = i, TotalChunks = totalChunks };
        }
    }

    private IEnumerable<(string Text, int StartOffset, int EndOffset)> SplitText(string text, int separatorIndex)
    {
        if (string.IsNullOrEmpty(text))
            yield break;

        // If the text is small enough, return it
        if (text.Length <= _chunkSize)
        {
            var trimmed = _trimWhitespace ? text.Trim() : text;
            if (trimmed.Length >= _minChunkSize)
            {
                yield return (trimmed, 0, text.Length);
            }
            yield break;
        }

        // If we've exhausted all separators, just split by character
        if (separatorIndex >= _separators.Length)
        {
            for (var i = 0; i < text.Length; i += _chunkSize - _overlap)
            {
                var length = Math.Min(_chunkSize, text.Length - i);
                var chunk = text.Substring(i, length);
                var trimmed = _trimWhitespace ? chunk.Trim() : chunk;
                if (trimmed.Length >= _minChunkSize)
                {
                    yield return (trimmed, i, i + length);
                }
            }
            yield break;
        }

        var separator = _separators[separatorIndex];
        var parts = SplitWithSeparator(text, separator);

        // If only one part (no splits), try the next separator
        if (parts.Count <= 1)
        {
            foreach (var chunk in SplitText(text, separatorIndex + 1))
            {
                yield return chunk;
            }
            yield break;
        }

        // Process each part
        foreach (var part in parts)
        {
            if (part.Text.Length <= _chunkSize)
            {
                var trimmed = _trimWhitespace ? part.Text.Trim() : part.Text;
                if (trimmed.Length >= _minChunkSize)
                {
                    yield return (trimmed, part.StartOffset, part.EndOffset);
                }
            }
            else
            {
                // Recursively split with next separator
                foreach (var chunk in SplitText(part.Text, separatorIndex + 1))
                {
                    yield return (chunk.Text, part.StartOffset + chunk.StartOffset, part.StartOffset + chunk.EndOffset);
                }
            }
        }
    }

    private static List<(string Text, int StartOffset, int EndOffset)> SplitWithSeparator(string text, string separator)
    {
        var result = new List<(string, int, int)>();

        if (string.IsNullOrEmpty(separator))
        {
            // Split by character
            for (var i = 0; i < text.Length; i++)
            {
                result.Add((text[i].ToString(), i, i + 1));
            }
            return result;
        }

        var parts = text.Split([separator], StringSplitOptions.None);
        var currentOffset = 0;

        for (var i = 0; i < parts.Length; i++)
        {
            var part = parts[i];
            // Include the separator with the preceding part (except for the last part)
            if (i < parts.Length - 1)
            {
                result.Add((part + separator, currentOffset, currentOffset + part.Length + separator.Length));
            }
            else if (!string.IsNullOrEmpty(part))
            {
                result.Add((part, currentOffset, currentOffset + part.Length));
            }

            currentOffset += part.Length + (i < parts.Length - 1 ? separator.Length : 0);
        }

        return result;
    }

    private List<TextChunk> MergeChunksWithOverlap(List<(string Text, int StartOffset, int EndOffset)> rawChunks)
    {
        if (rawChunks.Count == 0)
            return [];

        var result = new List<TextChunk>();
        var currentText = new List<string>();
        var currentLength = 0;
        var startOffset = rawChunks[0].StartOffset;

        foreach (var chunk in rawChunks)
        {
            // If adding this chunk would exceed chunk size, finalize current
            if (currentLength + chunk.Text.Length > _chunkSize && currentText.Count > 0)
            {
                var text = string.Join("", currentText);
                if (_trimWhitespace)
                    text = text.Trim();

                if (text.Length >= _minChunkSize)
                {
                    result.Add(new TextChunk
                    {
                        Text = text,
                        Index = 0,
                        StartOffset = startOffset,
                        EndOffset = startOffset + currentLength,
                        TotalChunks = 0
                    });
                }

                // Apply overlap
                var (overlapTexts, overlapLength) = ApplyOverlap(currentText, currentLength);
                currentText = overlapTexts;
                currentLength = overlapLength;
                startOffset = chunk.StartOffset - currentLength;
            }

            currentText.Add(chunk.Text);
            currentLength += chunk.Text.Length;
        }

        // Finalize last chunk
        if (currentText.Count > 0)
        {
            var text = string.Join("", currentText);
            if (_trimWhitespace)
                text = text.Trim();

            if (text.Length >= _minChunkSize)
            {
                result.Add(new TextChunk
                {
                    Text = text,
                    Index = 0,
                    StartOffset = startOffset,
                    EndOffset = startOffset + currentLength,
                    TotalChunks = 0
                });
            }
        }

        return result;
    }

    private (List<string> Texts, int Length) ApplyOverlap(List<string> texts, int totalLength)
    {
        if (_overlap == 0)
            return ([], 0);

        var result = new List<string>();
        var runningLength = 0;

        for (var i = texts.Count - 1; i >= 0 && runningLength < _overlap; i--)
        {
            result.Insert(0, texts[i]);
            runningLength += texts[i].Length;
        }

        return (result, runningLength);
    }
}
