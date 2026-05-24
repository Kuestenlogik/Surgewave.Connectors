using Kuestenlogik.Surgewave.Connector.TextChunking;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.TextChunking.Tests;

/// <summary>
/// Tests for the FixedSizeChunker class.
/// </summary>
public sealed class FixedSizeChunkerTests
{
    [Fact]
    public void Chunk_EmptyText_ReturnsNoChunks()
    {
        var chunker = new FixedSizeChunker(100, 20);
        var chunks = chunker.Chunk("").ToList();

        Assert.Empty(chunks);
    }

    [Fact]
    public void Chunk_NullText_ReturnsNoChunks()
    {
        var chunker = new FixedSizeChunker(100, 20);
        var chunks = chunker.Chunk(null!).ToList();

        Assert.Empty(chunks);
    }

    [Fact]
    public void Chunk_SmallText_ReturnsSingleChunk()
    {
        var chunker = new FixedSizeChunker(100, 20);
        var text = "Hello, world!";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal("Hello, world!", chunks[0].Text);
        Assert.Equal(0, chunks[0].Index);
        Assert.Equal(1, chunks[0].TotalChunks);
    }

    [Fact]
    public void Chunk_ExactSizeText_ReturnsSingleChunk()
    {
        var chunker = new FixedSizeChunker(10, 0);
        var text = "1234567890";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal("1234567890", chunks[0].Text);
    }

    [Fact]
    public void Chunk_LargeText_SplitsIntoMultipleChunks()
    {
        var chunker = new FixedSizeChunker(10, 0);
        var text = "12345678901234567890";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Equal(2, chunks.Count);
        Assert.Equal("1234567890", chunks[0].Text);
        Assert.Equal("1234567890", chunks[1].Text);
    }

    [Fact]
    public void Chunk_WithOverlap_HasOverlappingContent()
    {
        var chunker = new FixedSizeChunker(10, 3);
        var text = "ABCDEFGHIJKLMNOPQRST";
        var chunks = chunker.Chunk(text).ToList();

        // With size 10 and overlap 3, step is 7
        // Chunk 1: ABCDEFGHIJ (0-10)
        // Chunk 2: HIJKLMNOPQ (7-17)
        // Chunk 3: OPQRST (14-20)

        Assert.Equal(3, chunks.Count);
        Assert.Equal("ABCDEFGHIJ", chunks[0].Text);
        Assert.Equal("HIJKLMNOPQ", chunks[1].Text);
        Assert.Equal("OPQRST", chunks[2].Text);
    }

    [Fact]
    public void Chunk_SetsCorrectOffsets()
    {
        var chunker = new FixedSizeChunker(10, 0);
        var text = "123456789012345";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Equal(0, chunks[0].StartOffset);
        Assert.Equal(10, chunks[0].EndOffset);
        Assert.Equal(10, chunks[1].StartOffset);
        Assert.Equal(15, chunks[1].EndOffset);
    }

    [Fact]
    public void Chunk_SetsCorrectTotalChunks()
    {
        var chunker = new FixedSizeChunker(10, 0);
        var text = "123456789012345678901234567890";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Equal(3, chunks.Count);
        foreach (var chunk in chunks)
        {
            Assert.Equal(3, chunk.TotalChunks);
        }
    }

    [Fact]
    public void Chunk_ByWords_SplitsByWordCount()
    {
        var chunker = new FixedSizeChunker(3, 0, byWords: true);
        var text = "one two three four five six";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Equal(2, chunks.Count);
        Assert.Equal("one two three", chunks[0].Text);
        Assert.Equal("four five six", chunks[1].Text);
    }

    [Fact]
    public void Chunk_ByWordsWithOverlap_HasOverlappingWords()
    {
        var chunker = new FixedSizeChunker(3, 1, byWords: true);
        var text = "one two three four five six";
        var chunks = chunker.Chunk(text).ToList();

        // Chunks: [one two three], [three four five], [five six]
        Assert.Equal(3, chunks.Count);
        Assert.Equal("one two three", chunks[0].Text);
        Assert.Equal("three four five", chunks[1].Text);
        Assert.Equal("five six", chunks[2].Text);
    }

    [Fact]
    public void Chunk_TrimsWhitespace()
    {
        var chunker = new FixedSizeChunker(15, 0, trimWhitespace: true);
        var text = "  hello world  ";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal("hello world", chunks[0].Text);
    }

    [Fact]
    public void Chunk_WithMinChunkSize_SkipsSmallChunks()
    {
        var chunker = new FixedSizeChunker(10, 0, minChunkSize: 5);
        var text = "1234567890abc";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal("1234567890", chunks[0].Text);
    }

    [Fact]
    public void Constructor_ThrowsOnInvalidChunkSize()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new FixedSizeChunker(0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new FixedSizeChunker(-1, 0));
    }

    [Fact]
    public void Constructor_ThrowsOnNegativeOverlap()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new FixedSizeChunker(10, -1));
    }

    [Fact]
    public void Constructor_ThrowsOnOverlapGreaterThanSize()
    {
        Assert.Throws<ArgumentException>(() => new FixedSizeChunker(10, 10));
        Assert.Throws<ArgumentException>(() => new FixedSizeChunker(10, 11));
    }
}
