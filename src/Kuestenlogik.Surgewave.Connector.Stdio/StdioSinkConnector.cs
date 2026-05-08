using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Stdio;

/// <summary>
/// A sink connector that writes records to stdout or stderr.
/// Useful for CLI pipelines and scripting (e.g., surgewave-connect stdout | grep pattern).
/// </summary>
[ConnectorMetadata(
    Name = "Stdio Sink",
    Description = "Writes records to stdout or stderr. Useful for CLI pipelines and scripting.",
    Author = "KL Surgewave",
    Tags = "stdio,stdout,stderr,console,cli,pipe",
    Icon = "ConsoleLine")]
public sealed class StdioSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(StdioSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(StdioConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Topics to consume from")
        .Define(StdioConnectorConfig.OutputFormat, ConfigType.String,
            StdioConnectorConfig.DefaultOutputFormat, Importance.Medium,
            "Output format: 'line' (value only) or 'json' (structured with metadata)")
        .Define(StdioConnectorConfig.OutputTarget, ConfigType.String,
            StdioConnectorConfig.DefaultOutputTarget, Importance.Medium,
            "Output target: 'stdout' or 'stderr'")
        .Define(StdioConnectorConfig.IncludeKey, ConfigType.Boolean,
            StdioConnectorConfig.DefaultIncludeKey, Importance.Low,
            "Include message key in output (line format only)")
        .Define(StdioConnectorConfig.IncludeMetadata, ConfigType.Boolean,
            StdioConnectorConfig.DefaultIncludeMetadata, Importance.Low,
            "Include topic/partition/offset in JSON output")
        .Define(StdioConnectorConfig.KeyValueSeparator, ConfigType.String,
            StdioConnectorConfig.DefaultKeyValueSeparator, Importance.Low,
            "Separator between key and value (line format with include.key=true)");

    private string _topics = "";
    private string _outputFormat = StdioConnectorConfig.DefaultOutputFormat;
    private string _outputTarget = StdioConnectorConfig.DefaultOutputTarget;
    private bool _includeKey = StdioConnectorConfig.DefaultIncludeKey;
    private bool _includeMetadata = StdioConnectorConfig.DefaultIncludeMetadata;
    private string _keyValueSeparator = StdioConnectorConfig.DefaultKeyValueSeparator;

    public override void Start(IDictionary<string, string> config)
    {
        _topics = config.TryGetValue(StdioConnectorConfig.Topics, out var topics)
            ? topics
            : throw new ArgumentException($"Missing required config: {StdioConnectorConfig.Topics}");

        if (config.TryGetValue(StdioConnectorConfig.OutputFormat, out var format))
            _outputFormat = format;

        if (config.TryGetValue(StdioConnectorConfig.OutputTarget, out var target))
            _outputTarget = target;

        if (config.TryGetValue(StdioConnectorConfig.IncludeKey, out var includeKey))
            _includeKey = bool.Parse(includeKey);

        if (config.TryGetValue(StdioConnectorConfig.IncludeMetadata, out var includeMetadata))
            _includeMetadata = bool.Parse(includeMetadata);

        if (config.TryGetValue(StdioConnectorConfig.KeyValueSeparator, out var separator))
            _keyValueSeparator = separator;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Stdio only supports a single task (stdout is a single stream)
        return
        [
            new Dictionary<string, string>
            {
                [StdioConnectorConfig.Topics] = _topics,
                [StdioConnectorConfig.OutputFormat] = _outputFormat,
                [StdioConnectorConfig.OutputTarget] = _outputTarget,
                [StdioConnectorConfig.IncludeKey] = _includeKey.ToString(),
                [StdioConnectorConfig.IncludeMetadata] = _includeMetadata.ToString(),
                [StdioConnectorConfig.KeyValueSeparator] = _keyValueSeparator
            }
        ];
    }
}
