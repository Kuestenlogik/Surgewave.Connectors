namespace Kuestenlogik.Surgewave.Connector.Parquet;

/// <summary>
/// Configuration constants for Parquet connectors.
/// Supports Apache Parquet columnar format with compression.
/// </summary>
public static class ParquetConnectorConfig
{
    // Source settings
    public const string FilePath = "parquet.file.path";
    public const string Topic = "parquet.topic";
    public const string BatchSize = "parquet.batch.size";
    public const string PollIntervalMs = "parquet.poll.interval.ms";
    public const string DeleteAfterRead = "parquet.delete.after.read";
    public const string MoveAfterRead = "parquet.move.after.read";
    public const string ProcessedDirectory = "parquet.processed.directory";

    // Sink settings
    public const string Topics = "topics";
    public const string OutputPath = "parquet.output.path";
    public const string OutputMode = "parquet.output.mode";
    public const string MaxRecordsPerFile = "parquet.max.records.per.file";
    public const string FileNamePattern = "parquet.file.name.pattern";
    public const string CompressionCodec = "parquet.compression.codec";
    public const string RowGroupSize = "parquet.row.group.size";

    // Defaults
    public const int DefaultBatchSize = 1000;
    public const long DefaultPollIntervalMs = 1000;
    public const bool DefaultDeleteAfterRead = false;
    public const bool DefaultMoveAfterRead = false;

    public const string DefaultOutputMode = OutputModeAppend;
    public const int DefaultMaxRecordsPerFile = 0;
    public const string DefaultFileNamePattern = "${topic}-${timestamp}.parquet";
    public const string DefaultCompressionCodec = CompressionGzip;
    public const int DefaultRowGroupSize = 5000;

    // Output modes
    public const string OutputModeAppend = "append";
    public const string OutputModeOverwrite = "overwrite";
    public const string OutputModeRolling = "rolling";

    // Compression codecs
    public const string CompressionNone = "none";
    public const string CompressionGzip = "gzip";
    public const string CompressionSnappy = "snappy";
    public const string CompressionLz4 = "lz4";
    public const string CompressionZstd = "zstd";
    public const string CompressionBrotli = "brotli";

    // Offset keys
    public const string OffsetFilePath = "file";
    public const string OffsetRowIndex = "row";
    public const string OffsetFileModified = "modified";
}
