using Kuestenlogik.Surgewave.Connector.TextChunking;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.TextChunking.Tests;

/// <summary>
/// Tests for the ParagraphChunker class.
/// </summary>
public sealed class ParagraphChunkerTests
{
    [Fact]
    public void Chunk_EmptyText_ReturnsNoChunks()
    {
        var chunker = new ParagraphChunker(100, 0);
        var chunks = chunker.Chunk("").ToList();

        Assert.Empty(chunks);
    }

    [Fact]
    public void Chunk_SingleParagraph_ReturnsSingleChunk()
    {
        var chunker = new ParagraphChunker(100, 0);
        var text = "This is a single paragraph with no breaks.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal("This is a single paragraph with no breaks.", chunks[0].Text);
    }

    [Fact]
    public void Chunk_MultipleParagraphs_KeepsTogetherUnderLimit()
    {
        var chunker = new ParagraphChunker(200, 0);
        var text = "First paragraph.\n\nSecond paragraph.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Contains("First paragraph.", chunks[0].Text);
        Assert.Contains("Second paragraph.", chunks[0].Text);
    }

    [Fact]
    public void Chunk_LargeParagraphs_SplitsAtBoundaries()
    {
        var chunker = new ParagraphChunker(30, 0);
        var text = "First paragraph here.\n\nSecond paragraph here.\n\nThird paragraph here.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.True(chunks.Count >= 2);
    }

    [Fact]
    public void Chunk_WithCustomSeparator_UsesCustom()
    {
        var chunker = new ParagraphChunker(200, 0, separator: "---");
        var text = "First section---Second section---Third section";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Contains("First section", chunks[0].Text);
        Assert.Contains("Second section", chunks[0].Text);
        Assert.Contains("Third section", chunks[0].Text);
    }

    [Fact]
    public void Chunk_WithOverlap_IncludesOverlappingParagraphs()
    {
        var chunker = new ParagraphChunker(50, 25);
        var text = "Para one here.\n\nPara two here.\n\nPara three here.\n\nPara four here.";
        var chunks = chunker.Chunk(text).ToList();

        // With overlap, some paragraphs should appear in multiple chunks
        Assert.True(chunks.Count >= 2);
    }

    [Fact]
    public void Chunk_EmptyParagraphs_SkipsEmpty()
    {
        var chunker = new ParagraphChunker(100, 0);
        var text = "First paragraph.\n\n\n\nSecond paragraph.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        // Empty paragraphs should be filtered out
    }

    [Fact]
    public void Chunk_SetsCorrectMetadata()
    {
        var chunker = new ParagraphChunker(100, 0);
        var text = "First.\n\nSecond.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.Equal(0, chunks[0].Index);
        Assert.Equal(1, chunks[0].TotalChunks);
    }

    [Fact]
    public void Chunk_TrimsWhitespace()
    {
        var chunker = new ParagraphChunker(100, 0, trimWhitespace: true);
        var text = "  First paragraph.  \n\n  Second paragraph.  ";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.StartsWith("First", chunks[0].Text);
    }

    [Fact]
    public void Chunk_WithMinChunkSize_SkipsSmallParagraphs()
    {
        var chunker = new ParagraphChunker(100, 0, minChunkSize: 10);
        var text = "Hi\n\nThis is a longer paragraph.";
        var chunks = chunker.Chunk(text).ToList();

        Assert.Single(chunks);
        Assert.DoesNotContain("Hi", chunks[0].Text);
        Assert.Contains("longer paragraph", chunks[0].Text);
    }

    [Fact]
    public void Constructor_ThrowsOnInvalidMaxChunkSize()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ParagraphChunker(0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ParagraphChunker(-1, 0));
    }

    [Fact]
    public void Constructor_ThrowsOnNegativeOverlap()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ParagraphChunker(100, -1));
    }
}
