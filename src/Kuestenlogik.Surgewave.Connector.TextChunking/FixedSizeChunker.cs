namespace Kuestenlogik.Surgewave.Connector.TextChunking;

/// <summary>
/// Splits text into fixed-size chunks by character or word count.
/// </summary>
public sealed class FixedSizeChunker : ITextChunker
{
    private readonly int _chunkSize;
    private readonly int _overlap;
    private readonly bool _byWords;
    private readonly bool _trimWhitespace;
    private readonly int _minChunkSize;

    public FixedSizeChunker(int chunkSize, int overlap, bool byWords = false, bool trimWhitespace = true, int minChunkSize = 1)
    {
        if (chunkSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be positive");
        if (overlap < 0)
            throw new ArgumentOutOfRangeException(nameof(overlap), "Overlap cannot be negative");
        if (overlap >= chunkSize)
            throw new ArgumentException("Overlap must be less than chunk size", nameof(overlap));

        _chunkSize = chunkSize;
        _overlap = overlap;
        _byWords = byWords;
        _trimWhitespace = trimWhitespace;
        _minChunkSize = minChunkSize;
    }

    public IEnumerable<TextChunk> Chunk(string text)
    {
        if (string.IsNullOrEmpty(text))
            yield break;

        if (_byWords)
        {
            foreach (var chunk in ChunkByWords(text))
                yield return chunk;
        }
        else
        {
            foreach (var chunk in ChunkByCharacters(text))
                yield return chunk;
        }
    }

    private IEnumerable<TextChunk> ChunkByCharacters(string text)
    {
        var chunks = new List<TextChunk>();
        var step = _chunkSize - _overlap;
        var position = 0;
        var index = 0;

        while (position < text.Length)
        {
            var length = Math.Min(_chunkSize, text.Length - position);
            var chunkText = text.Substring(position, length);

            if (_trimWhitespace)
                chunkText = chunkText.Trim();

            if (chunkText.Length >= _minChunkSize)
            {
                chunks.Add(new TextChunk
                {
                    Text = chunkText,
                    Index = index++,
                    StartOffset = position,
                    EndOffset = position + length,
                    TotalChunks = 0 // Will be set after enumeration
                });
            }

            position += step;
        }

        // Update total chunks count
        var totalChunks = chunks.Count;
        return chunks.Select(c => c with { TotalChunks = totalChunks });
    }

    private IEnumerable<TextChunk> ChunkByWords(string text)
    {
        var words = SplitIntoWords(text);
        if (words.Count == 0)
            yield break;

        var chunks = new List<TextChunk>();
        var step = _chunkSize - _overlap;
        var wordIndex = 0;
        var chunkIndex = 0;

        while (wordIndex < words.Count)
        {
            var endWordIndex = Math.Min(wordIndex + _chunkSize, words.Count);
            var chunkWords = words.Skip(wordIndex).Take(endWordIndex - wordIndex).ToList();

            var chunkText = string.Join(" ", chunkWords.Select(w => w.Word));
            if (_trimWhitespace)
                chunkText = chunkText.Trim();

            if (chunkText.Length >= _minChunkSize)
            {
                var startOffset = chunkWords.First().StartOffset;
                var lastWord = chunkWords.Last();
                var endOffset = lastWord.StartOffset + lastWord.Word.Length;

                chunks.Add(new TextChunk
                {
                    Text = chunkText,
                    Index = chunkIndex++,
                    StartOffset = startOffset,
                    EndOffset = endOffset,
                    TotalChunks = 0
                });
            }

            wordIndex += step;
        }

        var totalChunks = chunks.Count;
        foreach (var chunk in chunks)
        {
            yield return chunk with { TotalChunks = totalChunks };
        }
    }

    private static List<(string Word, int StartOffset)> SplitIntoWords(string text)
    {
        var words = new List<(string, int)>();
        var wordStart = -1;

        for (var i = 0; i < text.Length; i++)
        {
            if (char.IsWhiteSpace(text[i]))
            {
                if (wordStart >= 0)
                {
                    words.Add((text.Substring(wordStart, i - wordStart), wordStart));
                    wordStart = -1;
                }
            }
            else if (wordStart < 0)
            {
                wordStart = i;
            }
        }

        if (wordStart >= 0)
        {
            words.Add((text.Substring(wordStart), wordStart));
        }

        return words;
    }
}
