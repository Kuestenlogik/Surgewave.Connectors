using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Csv;

/// <summary>
/// Source connector that reads records from CSV files.
/// Supports RFC 4180 compliant files with configurable delimiters,
/// headers, encoding, and file processing options.
/// </summary>
public sealed class CsvSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(CsvSourceTask);

    private Dictionary<string, string> _config = new();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(CsvConnectorConfig.FilePath, ConfigType.String, "", Importance.High,
            "Path to CSV file or directory to read from", EditorHint.FilePath)
        .Define(CsvConnectorConfig.FilePattern, ConfigType.String, "*.csv", Importance.Medium,
            "File pattern to match when reading from directory (e.g., '*.csv', 'data-*.csv')")
        .Define(CsvConnectorConfig.Topic, ConfigType.String, "", Importance.High,
            "Topic to publish records to", EditorHint.Topic)
        .Define(CsvConnectorConfig.Delimiter, ConfigType.String, CsvConnectorConfig.DefaultDelimiter, Importance.Medium,
            "Field delimiter character (default: comma)")
        .Define(CsvConnectorConfig.HasHeader, ConfigType.Boolean, CsvConnectorConfig.DefaultHasHeader, Importance.Medium,
            "Whether the CSV file has a header row")
        .Define(CsvConnectorConfig.Encoding, ConfigType.String, CsvConnectorConfig.DefaultEncoding, Importance.Low,
            "File encoding (e.g., 'utf-8', 'utf-16', 'iso-8859-1')")
        .Define(CsvConnectorConfig.KeyField, ConfigType.String, "", Importance.Medium,
            "Field to use as message key (optional)")
        .Define(CsvConnectorConfig.PollIntervalMs, ConfigType.Long, CsvConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Interval in milliseconds to poll for new files/data")
        .Define(CsvConnectorConfig.StartFromBeginning, ConfigType.Boolean, CsvConnectorConfig.DefaultStartFromBeginning, Importance.Medium,
            "Start reading from beginning of file or resume from last offset")
        .Define(CsvConnectorConfig.DeleteAfterRead, ConfigType.Boolean, CsvConnectorConfig.DefaultDeleteAfterRead, Importance.Medium,
            "Delete file after reading all records")
        .Define(CsvConnectorConfig.MoveAfterRead, ConfigType.Boolean, false, Importance.Medium,
            "Move file to processed directory after reading")
        .Define(CsvConnectorConfig.ProcessedDirectory, ConfigType.String, "", Importance.Low,
            "Directory to move processed files to", EditorHint.FilePath)
        .Define(CsvConnectorConfig.TrimFields, ConfigType.Boolean, CsvConnectorConfig.DefaultTrimFields, Importance.Low,
            "Trim whitespace from field values")
        .Define(CsvConnectorConfig.IgnoreBlankLines, ConfigType.Boolean, CsvConnectorConfig.DefaultIgnoreBlankLines, Importance.Low,
            "Skip blank lines in CSV file");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        if (!config.TryGetValue(CsvConnectorConfig.FilePath, out var filePath) || string.IsNullOrEmpty(filePath))
            throw new ArgumentException($"Missing required config: {CsvConnectorConfig.FilePath}");

        if (!config.TryGetValue(CsvConnectorConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
            throw new ArgumentException($"Missing required config: {CsvConnectorConfig.Topic}");
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // For file-based connector, typically use single task
        // Could partition by file if reading from directory
        var filePath = _config.TryGetValue(CsvConnectorConfig.FilePath, out var fp) ? fp : "";

        if (Directory.Exists(filePath))
        {
            var pattern = _config.TryGetValue(CsvConnectorConfig.FilePattern, out var pat) ? pat : "*.csv";
            var files = Directory.GetFiles(filePath, pattern);
            var tasksToCreate = Math.Min(maxTasks, Math.Max(1, files.Length));

            var configs = new List<IDictionary<string, string>>();
            var filesPerTask = files.Length / tasksToCreate;
            var remainder = files.Length % tasksToCreate;

            var fileIndex = 0;
            for (var i = 0; i < tasksToCreate; i++)
            {
                var count = filesPerTask + (i < remainder ? 1 : 0);
                var taskFiles = files.Skip(fileIndex).Take(count).ToArray();
                fileIndex += count;

                var taskConfig = new Dictionary<string, string>(_config)
                {
                    [CsvConnectorConfig.FilePath] = string.Join(";", taskFiles)
                };
                configs.Add(taskConfig);
            }

            return configs;
        }

        // Single file - single task
        return [new Dictionary<string, string>(_config)];
    }
}
