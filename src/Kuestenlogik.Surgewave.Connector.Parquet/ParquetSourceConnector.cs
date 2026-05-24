using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Parquet;

/// <summary>
/// Source connector that reads records from Apache Parquet files.
/// Supports schema inference, batch reading, and compression.
/// </summary>
public sealed class ParquetSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ParquetSourceTask);

    private Dictionary<string, string> _config = new();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(ParquetConnectorConfig.FilePath, ConfigType.String, "", Importance.High,
            "Path to Parquet file(s). Supports ';' delimited list", EditorHint.FilePath)
        .Define(ParquetConnectorConfig.Topic, ConfigType.String, "", Importance.High,
            "Target topic for records", EditorHint.Topic)
        .Define(ParquetConnectorConfig.BatchSize, ConfigType.Int, ParquetConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of rows to read per batch")
        .Define(ParquetConnectorConfig.PollIntervalMs, ConfigType.Long, ParquetConnectorConfig.DefaultPollIntervalMs, Importance.Low,
            "Poll interval in milliseconds when waiting for files")
        .Define(ParquetConnectorConfig.DeleteAfterRead, ConfigType.Boolean, ParquetConnectorConfig.DefaultDeleteAfterRead, Importance.Medium,
            "Delete file after processing")
        .Define(ParquetConnectorConfig.MoveAfterRead, ConfigType.Boolean, ParquetConnectorConfig.DefaultMoveAfterRead, Importance.Medium,
            "Move file to processed directory after reading")
        .Define(ParquetConnectorConfig.ProcessedDirectory, ConfigType.String, "", Importance.Low,
            "Directory to move processed files to", EditorHint.FilePath);

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        if (!config.TryGetValue(ParquetConnectorConfig.FilePath, out var filePath) || string.IsNullOrEmpty(filePath))
            throw new ArgumentException($"Missing required config: {ParquetConnectorConfig.FilePath}");

        if (!config.TryGetValue(ParquetConnectorConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
            throw new ArgumentException($"Missing required config: {ParquetConnectorConfig.Topic}");
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Distribute files across tasks
        var filePaths = _config.TryGetValue(ParquetConnectorConfig.FilePath, out var fp)
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
                [ParquetConnectorConfig.FilePath] = string.Join(";", taskFiles)
            };
            configs.Add(taskConfig);
        }

        return configs;
    }
}
