namespace Kuestenlogik.Surgewave.Connector.Excel;

/// <summary>
/// Configuration constants for Excel connectors.
/// Supports .xlsx files with sheet selection and cell range mapping.
/// </summary>
public static class ExcelConnectorConfig
{
    // Source settings
    public const string FilePath = "excel.file.path";
    public const string Topic = "excel.topic";
    public const string SheetName = "excel.sheet.name";
    public const string SheetIndex = "excel.sheet.index";
    public const string HasHeader = "excel.has.header";
    public const string StartRow = "excel.start.row";
    public const string EndRow = "excel.end.row";
    public const string StartColumn = "excel.start.column";
    public const string EndColumn = "excel.end.column";
    public const string KeyColumn = "excel.key.column";
    public const string BatchSize = "excel.batch.size";
    public const string PollIntervalMs = "excel.poll.interval.ms";
    public const string DeleteAfterRead = "excel.delete.after.read";
    public const string MoveAfterRead = "excel.move.after.read";
    public const string ProcessedDirectory = "excel.processed.directory";

    // Sink settings
    public const string Topics = "topics";
    public const string OutputPath = "excel.output.path";
    public const string OutputMode = "excel.output.mode";
    public const string OutputSheetName = "excel.output.sheet.name";
    public const string IncludeHeader = "excel.include.header";
    public const string MaxRowsPerFile = "excel.max.rows.per.file";
    public const string FileNamePattern = "excel.file.name.pattern";

    // Defaults
    public const bool DefaultHasHeader = true;
    public const int DefaultStartRow = 1;
    public const int DefaultStartColumn = 1;
    public const int DefaultBatchSize = 1000;
    public const long DefaultPollIntervalMs = 1000;
    public const bool DefaultDeleteAfterRead = false;
    public const bool DefaultMoveAfterRead = false;

    public const string DefaultOutputMode = OutputModeOverwrite;
    public const string DefaultOutputSheetName = "Sheet1";
    public const bool DefaultIncludeHeader = true;
    public const int DefaultMaxRowsPerFile = 0;
    public const string DefaultFileNamePattern = "${topic}-${timestamp}.xlsx";

    // Output modes
    public const string OutputModeAppend = "append";
    public const string OutputModeOverwrite = "overwrite";
    public const string OutputModeRolling = "rolling";

    // Offset keys
    public const string OffsetFilePath = "file";
    public const string OffsetSheetName = "sheet";
    public const string OffsetRowIndex = "row";
    public const string OffsetFileModified = "modified";
}
