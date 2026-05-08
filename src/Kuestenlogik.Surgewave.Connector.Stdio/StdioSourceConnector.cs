using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Stdio;

/// <summary>
/// A source connector that reads lines from stdin and produces them to a topic.
/// Useful for CLI pipelines and scripting (e.g., cat file.txt | surgewave-connect stdin).
/// </summary>
[ConnectorMetadata(
    Name = "Stdio Source",
    Description = "Reads lines from stdin and produces them to a topic. Useful for CLI pipelines and scripting.",
    Author = "KL Surgewave",
    Tags = "stdio,stdin,console,cli,pipe",
    Icon = "Console")]
public sealed class StdioSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(StdioSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(StdioConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Topic to write records to")
        .Define(StdioConnectorConfig.InputFormat, ConfigType.String,
            StdioConnectorConfig.DefaultInputFormat, Importance.Medium,
            "Input format: 'line' (each line is a record) or 'json' (parse JSON objects)");

    private string _topic = "";
    private string _inputFormat = StdioConnectorConfig.DefaultInputFormat;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config.TryGetValue(StdioConnectorConfig.Topic, out var topic)
            ? topic
            : throw new ArgumentException($"Missing required config: {StdioConnectorConfig.Topic}");

        if (config.TryGetValue(StdioConnectorConfig.InputFormat, out var format))
        {
            _inputFormat = format;
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Stdio only supports a single task (stdin is a single stream)
        return
        [
            new Dictionary<string, string>
            {
                [StdioConnectorConfig.Topic] = _topic,
                [StdioConnectorConfig.InputFormat] = _inputFormat
            }
        ];
    }
}
