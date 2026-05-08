namespace Kuestenlogik.Surgewave.Connector.TextChunking;

/// <summary>
/// Configuration constants for Text Chunking connectors.
/// </summary>
public static class TextChunkingConfig
{
    // Topics
    public const string Topics = "topics";
    public const string OutputTopic = "output.topic";

    // Chunking Strategy
    public const string Strategy = "chunking.strategy";
    public const string StrategyFixedSize = "fixed-size";
    public const string StrategySentence = "sentence";
    public const string StrategyParagraph = "paragraph";
    public const string StrategyRecursive = "recursive";
    public const string DefaultStrategy = StrategyFixedSize;

    // Fixed Size Settings
    public const string ChunkSize = "chunk.size";
    public const int DefaultChunkSize = 1000;
    public const string ChunkOverlap = "chunk.overlap";
    public const int DefaultChunkOverlap = 200;
    public const string ChunkUnit = "chunk.unit";
    public const string ChunkUnitCharacters = "characters";
    public const string ChunkUnitWords = "words";
    public const string DefaultChunkUnit = ChunkUnitCharacters;

    // Sentence/Paragraph Settings
    public const string SentenceDelimiters = "sentence.delimiters";
    public const string DefaultSentenceDelimiters = ".!?";
    public const string ParagraphSeparator = "paragraph.separator";
    public const string DefaultParagraphSeparator = "\n\n";

    // Recursive Strategy Settings
    public const string RecursiveSeparators = "recursive.separators";
    public const string DefaultRecursiveSeparators = "\n\n|\n| |";

    // Output Settings
    public const string IncludeMetadata = "include.metadata";
    public const bool DefaultIncludeMetadata = true;
    public const string MetadataPrefix = "metadata.prefix";
    public const string DefaultMetadataPrefix = "chunk_";

    // Processing
    public const string TrimWhitespace = "trim.whitespace";
    public const bool DefaultTrimWhitespace = true;
    public const string SkipEmpty = "skip.empty";
    public const bool DefaultSkipEmpty = true;
    public const string MinChunkSize = "min.chunk.size";
    public const int DefaultMinChunkSize = 1;
}
