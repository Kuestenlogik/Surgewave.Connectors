namespace Kuestenlogik.Surgewave.Connector.Batching;

/// <summary>
/// Configuration constants for the batching connector.
/// </summary>
public static class BatchingConnectorConfig
{
    // Topics
    public const string TopicsConfig = "topics";

    // Batching policy
    public const string BatchMaxMessagesConfig = "batch.max.messages";
    public const int DefaultBatchMaxMessages = 100;

    public const string BatchMaxBytesConfig = "batch.max.bytes";
    public const long DefaultBatchMaxBytes = 1048576; // 1MB

    public const string BatchTimeoutMsConfig = "batch.timeout.ms";
    public const int DefaultBatchTimeoutMs = 1000;

    // Batch output format
    public const string BatchFormatConfig = "batch.format";
    public const string FormatJsonArray = "json-array";
    public const string FormatJsonLines = "json-lines";
    public const string FormatRaw = "raw";
    public const string DefaultBatchFormat = FormatJsonArray;

    // Key handling
    public const string KeyStrategyConfig = "key.strategy";
    public const string KeyStrategyFirst = "first";
    public const string KeyStrategyLast = "last";
    public const string KeyStrategyNull = "null";
    public const string KeyStrategyConcat = "concat";
    public const string DefaultKeyStrategy = KeyStrategyFirst;

    // Metadata handling
    public const string IncludeMetadataConfig = "include.metadata";
    public const bool DefaultIncludeMetadata = false;

    // Separator for concatenation
    public const string SeparatorConfig = "separator";
    public const string DefaultSeparator = "\n";

    // Flush on key change
    public const string FlushOnKeyChangeConfig = "flush.on.key.change";
    public const bool DefaultFlushOnKeyChange = false;

    // Compression
    public const string CompressionConfig = "compression";
    public const string CompressionNone = "none";
    public const string CompressionGzip = "gzip";
    public const string DefaultCompression = CompressionNone;
}
