using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Csv;

/// <summary>
/// Sink connector that writes records to CSV files.
/// Supports RFC 4180 compliant output with configurable delimiters,
/// headers, and file rotation options.
/// </summary>
public sealed class CsvSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(CsvSinkTask);

    private Dictionary<string, string> _config = new();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(CsvConnectorConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(CsvConnectorConfig.OutputPath, ConfigType.String, "", Importance.High,
            "Output directory or file path for CSV files", EditorHint.FilePath)
        .Define(CsvConnectorConfig.Delimiter, ConfigType.String, CsvConnectorConfig.DefaultDelimiter, Importance.Medium,
            "Field delimiter character (default: comma)")
        .Define(CsvConnectorConfig.IncludeHeader, ConfigType.Boolean, CsvConnectorConfig.DefaultIncludeHeader, Importance.Medium,
            "Include header row in output file")
        .Define(CsvConnectorConfig.Encoding, ConfigType.String, CsvConnectorConfig.DefaultEncoding, Importance.Low,
            "File encoding (e.g., 'utf-8', 'utf-16', 'iso-8859-1')")
        .Define(CsvConnectorConfig.OutputMode, ConfigType.String, CsvConnectorConfig.DefaultOutputMode, Importance.Medium,
            "Output mode: 'append', 'overwrite', or 'rolling'", EditorHint.Select, options: ["append", "overwrite", "rolling"])
        .Define(CsvConnectorConfig.MaxRecordsPerFile, ConfigType.Int, CsvConnectorConfig.DefaultMaxRecordsPerFile, Importance.Medium,
            "Maximum records per file for rolling mode (0 = unlimited)")
        .Define(CsvConnectorConfig.FileNamePattern, ConfigType.String, CsvConnectorConfig.DefaultFileNamePattern, Importance.Medium,
            "File name pattern for rolling mode (supports ${topic}, ${timestamp}, ${partition})");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        if (!config.TryGetValue(CsvConnectorConfig.Topics, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Missing required config: {CsvConnectorConfig.Topics}");

        if (!config.TryGetValue(CsvConnectorConfig.OutputPath, out var outputPath) || string.IsNullOrEmpty(outputPath))
            throw new ArgumentException($"Missing required config: {CsvConnectorConfig.OutputPath}");

        // Validate output mode
        var mode = config.TryGetValue(CsvConnectorConfig.OutputMode, out var m) ? m : CsvConnectorConfig.DefaultOutputMode;
        if (mode is not (CsvConnectorConfig.OutputModeAppend or CsvConnectorConfig.OutputModeOverwrite or CsvConnectorConfig.OutputModeRolling))
            throw new ArgumentException($"Invalid output mode: {mode}. Must be 'append', 'overwrite', or 'rolling'");
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task for file writing to avoid conflicts
        return [new Dictionary<string, string>(_config)];
    }
}
