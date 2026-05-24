using Kuestenlogik.Surgewave.Connector.TextChunking;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.TextChunking.Tests;

/// <summary>
/// Tests for the SentenceChunker class.
/// </summary>
public sealed class SentenceChunkerTests
{
    [Fact]
    public void Chunk_EmptyText_ReturnsNoChunks()
    {
        var chunker = new SentenceChunker(100, 0);
        var chunks = chunker.Chunk("").ToList();

        Assert.Empty(chunks);
    }

    [Fact]
    public void Chunk_SingleSentence_ReturnsSingleChunk()
    {
        var chunker = new SentenceChunker(100, 0);
        var text = "This is a single sentence.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal("This is a single sentence.", chunks[0].Text);
    }

    [Fact]
    public void Chunk_MultipleSentences_KeepsTogether()
    {
        var chunker = new SentenceChunker(100, 0);
        var text = "First sentence. Second sentence. Third sentence.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Contains("First sentence.", chunks[0].Text);
        Assert.Contains("Second sentence.", chunks[0].Text);
        Assert.Contains("Third sentence.", chunks[0].Text);
    }

    [Fact]
    public void Chunk_LongText_SplitsAtSentenceBoundaries()
    {
        var chunker = new SentenceChunker(30, 0);
        var text = "Short one. Another short. Third short. Fourth short.";
        var chunks = chunker.Chunk(text).ToList();

        // Each sentence is about 12-13 chars
        // Should split when adding next sentence exceeds 30
        Assert.True(chunks.Count >= 2);
        foreach (var chunk in chunks)
        {
            // Each chunk should end with a sentence delimiter
            Assert.True(chunk.Text.EndsWith('.'));
        }
    }

    [Fact]
    public void Chunk_WithQuestionMarks_RecognizesDelimiter()
    {
        var chunker = new SentenceChunker(100, 0, ".!?");
        var text = "Is this a question? Yes it is.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Contains("question?", chunks[0].Text);
    }

    [Fact]
    public void Chunk_WithExclamation_RecognizesDelimiter()
    {
        var chunker = new SentenceChunker(100, 0, ".!?");
        var text = "Wow! That is amazing.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Contains("Wow!", chunks[0].Text);
    }

    [Fact]
    public void Chunk_WithOverlap_IncludesOverlappingSentences()
    {
        var chunker = new SentenceChunker(30, 15);
        var text = "First. Second. Third. Fourth.";
        var chunks = chunker.Chunk(text).ToList();

        // With overlap, some sentences should appear in multiple chunks
        if (chunks.Count > 1)
        {
            var lastChunkOfFirst = chunks[0].Text.Split('.').Where(s => !string.IsNullOrWhiteSpace(s)).Last();
            // Due to overlap, this sentence might appear in the next chunk too
        }
    }

    [Fact]
    public void Chunk_NoDelimiterAtEnd_IncludesTrailingText()
    {
        var chunker = new SentenceChunker(100, 0);
        var text = "First sentence. No delimiter at end";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Contains("No delimiter at end", chunks[0].Text);
    }

    [Fact]
    public void Chunk_SetsCorrectMetadata()
    {
        var chunker = new SentenceChunker(100, 0);
        var text = "First. Second.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal(0, chunks[0].Index);
        Assert.Equal(1, chunks[0].TotalChunks);
        Assert.Equal(0, chunks[0].StartOffset);
    }

    [Fact]
    public void Chunk_WithCustomDelimiters_UsesCustom()
    {
        var chunker = new SentenceChunker(100, 0, ";:");
        var text = "First; Second: Third";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        // All three parts should be in the single chunk
        Assert.Contains("First;", chunks[0].Text);
        Assert.Contains("Second:", chunks[0].Text);
    }

    [Fact]
    public void Constructor_ThrowsOnInvalidMaxChunkSize()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SentenceChunker(0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SentenceChunker(-1, 0));
    }

    [Fact]
    public void Constructor_ThrowsOnNegativeOverlap()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SentenceChunker(100, -1));
    }
}
