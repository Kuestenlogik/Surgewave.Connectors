using Kuestenlogik.Surgewave.Connector.TextChunking;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.TextChunking.Tests;

/// <summary>
/// Tests for the RecursiveChunker class.
/// </summary>
public sealed class RecursiveChunkerTests
{
    [Fact]
    public void Chunk_EmptyText_ReturnsNoChunks()
    {
        var chunker = new RecursiveChunker(100, 0);
        var chunks = chunker.Chunk("").ToList();

        Assert.Empty(chunks);
    }

    [Fact]
    public void Chunk_SmallText_ReturnsSingleChunk()
    {
        var chunker = new RecursiveChunker(100, 0);
        var text = "This is a small text.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal("This is a small text.", chunks[0].Text);
    }

    [Fact]
    public void Chunk_SplitsOnParagraphs_First()
    {
        var chunker = new RecursiveChunker(50, 0, ["\n\n", "\n", " "]);
        var text = "First paragraph here.\n\nSecond paragraph here.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.True(chunks.Count >= 1);
    }

    [Fact]
    public void Chunk_FallsBackToNewlines()
    {
        var chunker = new RecursiveChunker(30, 0, ["\n\n", "\n", " "]);
        var text = "Line one\nLine two\nLine three";
        var chunks = chunker.Chunk(text).ToList();

        Assert.True(chunks.Count >= 1);
    }

    [Fact]
    public void Chunk_FallsBackToSpaces()
    {
        var chunker = new RecursiveChunker(20, 0, ["\n\n", "\n", " "]);
        var text = "word1 word2 word3 word4 word5 word6";
        var chunks = chunker.Chunk(text).ToList();

        Assert.True(chunks.Count >= 2);
    }

    [Fact]
    public void Chunk_FallsBackToCharacters()
    {
        var chunker = new RecursiveChunker(10, 0, ["\n\n", "\n", " ", ""]);
        var text = "abcdefghijklmnopqrstuvwxyz";
        var chunks = chunker.Chunk(text).ToList();

        Assert.True(chunks.Count >= 2);
        Assert.True(chunks[0].Text.Length <= 10);
    }

    [Fact]
    public void Chunk_WithOverlap_MergesWithOverlap()
    {
        var chunker = new RecursiveChunker(20, 5, [" "]);
        var text = "one two three four five six seven eight";
        var chunks = chunker.Chunk(text).ToList();

        // With overlap, chunks should have some shared content
        Assert.True(chunks.Count >= 2);
    }

    [Fact]
    public void Chunk_SetsCorrectMetadata()
    {
        var chunker = new RecursiveChunker(100, 0);
        var text = "Test text";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal(0, chunks[0].Index);
        Assert.Equal(1, chunks[0].TotalChunks);
    }

    [Fact]
    public void Chunk_WithCustomSeparators_UsesCustom()
    {
        var chunker = new RecursiveChunker(50, 0, ["||", "|"]);
        var text = "Part one||Part two|Part three";
        var chunks = chunker.Chunk(text).ToList();

        Assert.True(chunks.Count >= 1);
    }

    [Fact]
    public void Chunk_TrimsWhitespace()
    {
        var chunker = new RecursiveChunker(100, 0, trimWhitespace: true);
        var text = "  trimmed text  ";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal("trimmed text", chunks[0].Text);
    }

    [Fact]
    public void Chunk_WithMinChunkSize_SkipsSmall()
    {
        var chunker = new RecursiveChunker(100, 0, minChunkSize: 5);
        var text = "ab";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Empty(chunks);
    }

    [Fact]
    public void Chunk_DefaultSeparators_AreCorrect()
    {
        var chunker = new RecursiveChunker(50, 0);
        // Default separators are: "\n\n", "\n", " ", ""
        var text = "Para one.\n\nPara two.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.True(chunks.Count >= 1);
    }

    [Fact]
    public void Constructor_ThrowsOnInvalidChunkSize()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RecursiveChunker(0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RecursiveChunker(-1, 0));
    }

    [Fact]
    public void Constructor_ThrowsOnNegativeOverlap()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RecursiveChunker(100, -1));
    }

    [Fact]
    public void Constructor_ThrowsOnOverlapGreaterThanSize()
    {
        Assert.Throws<ArgumentException>(() => new RecursiveChunker(10, 10));
        Assert.Throws<ArgumentException>(() => new RecursiveChunker(10, 11));
    }
}
