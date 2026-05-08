namespace Kuestenlogik.Surgewave.Connector.Stdio;

/// <summary>
/// Configuration constants for Stdio connectors.
/// </summary>
public static class StdioConnectorConfig
{
    // Source connector
    public const string Topic = "topic";
    public const string InputFormat = "input.format";
    public const string InputFormatLine = "line";
    public const string InputFormatJson = "json";
    public const string DefaultInputFormat = InputFormatLine;

    // Sink connector
    public const string Topics = "topics";
    public const string OutputFormat = "output.format";
    public const string OutputFormatLine = "line";
    public const string OutputFormatJson = "json";
    public const string DefaultOutputFormat = OutputFormatLine;

    public const string OutputTarget = "output.target";
    public const string OutputTargetStdout = "stdout";
    public const string OutputTargetStderr = "stderr";
    public const string DefaultOutputTarget = OutputTargetStdout;

    public const string IncludeKey = "include.key";
    public const bool DefaultIncludeKey = false;

    public const string IncludeMetadata = "include.metadata";
    public const bool DefaultIncludeMetadata = false;

    public const string KeyValueSeparator = "key.value.separator";
    public const string DefaultKeyValueSeparator = "\t";

    // Offset tracking
    public const string OffsetLineNumber = "line_number";
}
