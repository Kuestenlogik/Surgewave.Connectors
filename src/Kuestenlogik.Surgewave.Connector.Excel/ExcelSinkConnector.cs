using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Excel;

/// <summary>
/// Sink connector that writes records to Excel (.xlsx) files.
/// Supports sheet configuration, header row, and file rotation.
/// </summary>
public sealed class ExcelSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ExcelSinkTask);

    private Dictionary<string, string> _config = new();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(ExcelConnectorConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(ExcelConnectorConfig.OutputPath, ConfigType.String, "", Importance.High,
            "Output directory or file path for Excel files", EditorHint.FilePath)
        .Define(ExcelConnectorConfig.OutputMode, ConfigType.String, ExcelConnectorConfig.DefaultOutputMode, Importance.Medium,
            "Output mode: 'append', 'overwrite', or 'rolling'", EditorHint.Select, options: ["append", "overwrite", "rolling"])
        .Define(ExcelConnectorConfig.OutputSheetName, ConfigType.String, ExcelConnectorConfig.DefaultOutputSheetName, Importance.Medium,
            "Name of the worksheet to write to")
        .Define(ExcelConnectorConfig.IncludeHeader, ConfigType.Boolean, ExcelConnectorConfig.DefaultIncludeHeader, Importance.Medium,
            "Include header row in output file")
        .Define(ExcelConnectorConfig.MaxRowsPerFile, ConfigType.Int, ExcelConnectorConfig.DefaultMaxRowsPerFile, Importance.Medium,
            "Maximum rows per file for rolling mode (0 = unlimited)")
        .Define(ExcelConnectorConfig.FileNamePattern, ConfigType.String, ExcelConnectorConfig.DefaultFileNamePattern, Importance.Medium,
            "File name pattern for rolling mode (supports ${topic}, ${timestamp}, ${partition})");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        if (!config.TryGetValue(ExcelConnectorConfig.Topics, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Missing required config: {ExcelConnectorConfig.Topics}");

        if (!config.TryGetValue(ExcelConnectorConfig.OutputPath, out var outputPath) || string.IsNullOrEmpty(outputPath))
            throw new ArgumentException($"Missing required config: {ExcelConnectorConfig.OutputPath}");

        // Validate output mode
        var mode = config.TryGetValue(ExcelConnectorConfig.OutputMode, out var m) ? m : ExcelConnectorConfig.DefaultOutputMode;
        if (mode is not (ExcelConnectorConfig.OutputModeAppend or ExcelConnectorConfig.OutputModeOverwrite or ExcelConnectorConfig.OutputModeRolling))
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
