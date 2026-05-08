namespace Kuestenlogik.Surgewave.Connector.TextChunking;

/// <summary>
/// Splits text into chunks based on sentence boundaries.
/// </summary>
public sealed class SentenceChunker : ITextChunker
{
    private readonly int _maxChunkSize;
    private readonly int _overlap;
    private readonly char[] _delimiters;
    private readonly bool _trimWhitespace;
    private readonly int _minChunkSize;

    public SentenceChunker(int maxChunkSize, int overlap, string delimiters = ".!?", bool trimWhitespace = true, int minChunkSize = 1)
    {
        if (maxChunkSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxChunkSize), "Max chunk size must be positive");
        if (overlap < 0)
            throw new ArgumentOutOfRangeException(nameof(overlap), "Overlap cannot be negative");

        _maxChunkSize = maxChunkSize;
        _overlap = overlap;
        _delimiters = delimiters.ToCharArray();
        _trimWhitespace = trimWhitespace;
        _minChunkSize = minChunkSize;
    }

    public IEnumerable<TextChunk> Chunk(string text)
    {
        if (string.IsNullOrEmpty(text))
            yield break;

        var sentences = SplitIntoSentences(text);
        if (sentences.Count == 0)
            yield break;

        var chunks = new List<TextChunk>();
        var currentSentences = new List<(string Text, int StartOffset, int EndOffset)>();
        var currentLength = 0;
        var chunkIndex = 0;

        foreach (var sentence in sentences)
        {
            var sentenceLength = sentence.Text.Length;

            // If adding this sentence would exceed the max size, finalize current chunk
            if (currentLength + sentenceLength > _maxChunkSize && currentSentences.Count > 0)
            {
                chunks.Add(CreateChunk(currentSentences, chunkIndex++));

                // Apply overlap: keep some sentences for the next chunk
                currentSentences = ApplyOverlap(currentSentences);
                currentLength = currentSentences.Sum(s => s.Text.Length);
            }

            currentSentences.Add(sentence);
            currentLength += sentenceLength;
        }

        // Finalize the last chunk
        if (currentSentences.Count > 0)
        {
            chunks.Add(CreateChunk(currentSentences, chunkIndex));
        }

        var totalChunks = chunks.Count;
        foreach (var chunk in chunks)
        {
            yield return chunk with { TotalChunks = totalChunks };
        }
    }

    private TextChunk CreateChunk(List<(string Text, int StartOffset, int EndOffset)> sentences, int index)
    {
        var text = string.Join(" ", sentences.Select(s => s.Text));
        if (_trimWhitespace)
            text = text.Trim();

        return new TextChunk
        {
            Text = text,
            Index = index,
            StartOffset = sentences.First().StartOffset,
            EndOffset = sentences.Last().EndOffset,
            TotalChunks = 0
        };
    }

    private List<(string Text, int StartOffset, int EndOffset)> ApplyOverlap(List<(string Text, int StartOffset, int EndOffset)> sentences)
    {
        if (_overlap == 0 || sentences.Count <= 1)
            return [];

        // Calculate how many sentences to keep based on overlap
        var totalLength = sentences.Sum(s => s.Text.Length);
        var targetLength = Math.Min(_overlap, totalLength);
        var result = new List<(string Text, int StartOffset, int EndOffset)>();
        var runningLength = 0;

        // Work backwards to get sentences that fit in overlap
        for (var i = sentences.Count - 1; i >= 0 && runningLength < targetLength; i--)
        {
            result.Insert(0, sentences[i]);
            runningLength += sentences[i].Text.Length;
        }

        return result;
    }

    private List<(string Text, int StartOffset, int EndOffset)> SplitIntoSentences(string text)
    {
        var sentences = new List<(string, int, int)>();
        var start = 0;

        for (var i = 0; i < text.Length; i++)
        {
            if (Array.IndexOf(_delimiters, text[i]) >= 0)
            {
                // Include trailing whitespace with the sentence
                var end = i + 1;
                while (end < text.Length && char.IsWhiteSpace(text[end]))
                    end++;

                var sentence = text.Substring(start, end - start);
                if (_trimWhitespace)
                    sentence = sentence.Trim();

                if (sentence.Length >= _minChunkSize)
                {
                    sentences.Add((sentence, start, end));
                }

                start = end;
                i = end - 1;
            }
        }

        // Handle remaining text (no delimiter at end)
        if (start < text.Length)
        {
            var sentence = text.Substring(start);
            if (_trimWhitespace)
                sentence = sentence.Trim();

            if (sentence.Length >= _minChunkSize)
            {
                sentences.Add((sentence, start, text.Length));
            }
        }

        return sentences;
    }
}
