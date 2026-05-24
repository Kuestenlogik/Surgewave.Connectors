using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Excel;

/// <summary>
/// Source connector that reads records from Excel (.xlsx) files.
/// Supports sheet selection, cell range mapping, and header row detection.
/// </summary>
public sealed class ExcelSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ExcelSourceTask);

    private Dictionary<string, string> _config = new();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(ExcelConnectorConfig.FilePath, ConfigType.String, "", Importance.High,
            "Path to Excel file(s). Supports ';' delimited list", EditorHint.FilePath)
        .Define(ExcelConnectorConfig.Topic, ConfigType.String, "", Importance.High,
            "Target topic for records", EditorHint.Topic)
        .Define(ExcelConnectorConfig.SheetName, ConfigType.String, "", Importance.Medium,
            "Sheet name to read (optional, defaults to first sheet)")
        .Define(ExcelConnectorConfig.SheetIndex, ConfigType.Int, 1, Importance.Medium,
            "Sheet index to read (1-based, used if sheet name not specified)")
        .Define(ExcelConnectorConfig.HasHeader, ConfigType.Boolean, ExcelConnectorConfig.DefaultHasHeader, Importance.Medium,
            "Whether the first row contains column headers")
        .Define(ExcelConnectorConfig.StartRow, ConfigType.Int, ExcelConnectorConfig.DefaultStartRow, Importance.Low,
            "Starting row number (1-based)")
        .Define(ExcelConnectorConfig.EndRow, ConfigType.Int, 0, Importance.Low,
            "Ending row number (0 = read to end)")
        .Define(ExcelConnectorConfig.StartColumn, ConfigType.Int, ExcelConnectorConfig.DefaultStartColumn, Importance.Low,
            "Starting column number (1-based)")
        .Define(ExcelConnectorConfig.EndColumn, ConfigType.Int, 0, Importance.Low,
            "Ending column number (0 = read to end)")
        .Define(ExcelConnectorConfig.KeyColumn, ConfigType.String, "", Importance.Low,
            "Column name or number to use as message key")
        .Define(ExcelConnectorConfig.BatchSize, ConfigType.Int, ExcelConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of rows to read per batch")
        .Define(ExcelConnectorConfig.PollIntervalMs, ConfigType.Long, ExcelConnectorConfig.DefaultPollIntervalMs, Importance.Low,
            "Poll interval in milliseconds")
        .Define(ExcelConnectorConfig.DeleteAfterRead, ConfigType.Boolean, ExcelConnectorConfig.DefaultDeleteAfterRead, Importance.Medium,
            "Delete file after processing")
        .Define(ExcelConnectorConfig.MoveAfterRead, ConfigType.Boolean, ExcelConnectorConfig.DefaultMoveAfterRead, Importance.Medium,
            "Move file to processed directory after reading")
        .Define(ExcelConnectorConfig.ProcessedDirectory, ConfigType.String, "", Importance.Low,
            "Directory to move processed files to", EditorHint.FilePath);

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        if (!config.TryGetValue(ExcelConnectorConfig.FilePath, out var filePath) || string.IsNullOrEmpty(filePath))
            throw new ArgumentException($"Missing required config: {ExcelConnectorConfig.FilePath}");

        if (!config.TryGetValue(ExcelConnectorConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
            throw new ArgumentException($"Missing required config: {ExcelConnectorConfig.Topic}");
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Distribute files across tasks
        var filePaths = _config.TryGetValue(ExcelConnectorConfig.FilePath, out var fp)
            ? fp.Split(';', StringSplitOptions.RemoveEmptyEntries)
            : [];

        if (filePaths.Length == 0)
            return [new Dictionary<string, string>(_config)];

        var taskCount = Math.Min(maxTasks, filePaths.Length);
        var configs = new List<IDictionary<string, string>>();

        for (var i = 0; i < taskCount; i++)
        {
            var taskFiles = filePaths
                .Where((_, idx) => idx % taskCount == i)
                .ToArray();

            var taskConfig = new Dictionary<string, string>(_config)
            {
                [ExcelConnectorConfig.FilePath] = string.Join(";", taskFiles)
            };
            configs.Add(taskConfig);
        }

        return configs;
    }
}
