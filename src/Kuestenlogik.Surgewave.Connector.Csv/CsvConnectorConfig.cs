namespace Kuestenlogik.Surgewave.Connector.Csv;

/// <summary>
/// Configuration constants for CSV connectors.
/// Supports RFC 4180 compliant CSV files.
/// </summary>
public static class CsvConnectorConfig
{
    // Common config
    public const string FilePath = "file.path";
    public const string FilePattern = "file.pattern";
    public const string Delimiter = "delimiter";
    public const string HasHeader = "has.header";
    public const string Encoding = "encoding";
    public const string Quote = "quote.char";
    public const string Escape = "escape.char";
    public const string Comment = "comment.char";
    public const string TrimFields = "trim.fields";
    public const string IgnoreBlankLines = "ignore.blank.lines";

    // Source config
    public const string Topic = "topic";
    public const string PollIntervalMs = "poll.interval.ms";
    public const string KeyField = "key.field";
    public const string StartFromBeginning = "start.from.beginning";
    public const string DeleteAfterRead = "delete.after.read";
    public const string MoveAfterRead = "move.after.read";
    public const string ProcessedDirectory = "processed.directory";

    // Sink config
    public const string Topics = "topics";
    public const string OutputPath = "output.path";
    public const string OutputMode = "output.mode";
    public const string IncludeHeader = "include.header";
    public const string MaxRecordsPerFile = "max.records.per.file";
    public const string FileNamePattern = "file.name.pattern";

    // Output modes
    public const string OutputModeAppend = "append";
    public const string OutputModeOverwrite = "overwrite";
    public const string OutputModeRolling = "rolling";

    // Defaults
    public const string DefaultDelimiter = ",";
    public const bool DefaultHasHeader = true;
    public const string DefaultEncoding = "utf-8";
    public const char DefaultQuote = '"';
    public const char DefaultEscape = '"';
    public const bool DefaultTrimFields = false;
    public const bool DefaultIgnoreBlankLines = true;
    public const long DefaultPollIntervalMs = 1000;
    public const bool DefaultStartFromBeginning = true;
    public const bool DefaultDeleteAfterRead = false;
    public const bool DefaultIncludeHeader = true;
    public const string DefaultOutputMode = OutputModeAppend;
    public const int DefaultMaxRecordsPerFile = 0; // 0 = unlimited
    public const string DefaultFileNamePattern = "${topic}-${timestamp}.csv";

    // Offset keys
    public const string OffsetFilePath = "file_path";
    public const string OffsetLineNumber = "line_number";
    public const string OffsetFileModified = "file_modified";
}
