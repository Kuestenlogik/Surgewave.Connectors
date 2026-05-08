using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Logic;

/// <summary>
/// Merges records from multiple input topics into a single output topic.
/// Acts as a funnel for fan-in patterns.
/// </summary>
[ConnectorMetadata(
    Name = "Merge",
    Description = "Merge multiple input topics into a single output topic",
    Author = "Surgewave",
    Tags = "logic,merge,join,funnel",
    Icon = "merge")]
public sealed class MergeConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(MergeTask);

    private string _outputTopic = "";
    private readonly List<string> _inputTopics = [];
    private bool _addSourceHeader;

    public override ConfigDef Config => new ConfigDef()
        .Define(MergeConfig.OutputTopic, ConfigType.String, "", Importance.High,
            "Topic to send merged records to", EditorHint.Topic)
        .Define(MergeConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of input topics to merge", EditorHint.Topic)
        .Define(MergeConfig.AddSourceHeader, ConfigType.Boolean, "false", Importance.Low,
            "Add header with source topic name");

    public override void Start(IDictionary<string, string> config)
    {
        _outputTopic = config.GetValueOrDefault(MergeConfig.OutputTopic, "")
            ?? throw new ArgumentException("Output topic is required");

        var topics = config.GetValueOrDefault(MergeConfig.Topics, "") ?? "";
        _inputTopics.AddRange(topics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

        if (_inputTopics.Count == 0)
        {
            throw new ArgumentException("At least one input topic is required");
        }

        _addSourceHeader = bool.TryParse(config.GetValueOrDefault(MergeConfig.AddSourceHeader, "false"), out var b) && b;
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return
        [
            new Dictionary<string, string>
            {
                [MergeConfig.OutputTopic] = _outputTopic,
                [MergeConfig.Topics] = string.Join(",", _inputTopics),
                [MergeConfig.AddSourceHeader] = _addSourceHeader.ToString().ToLowerInvariant()
            }
        ];
    }
}

/// <summary>
/// Task that merges records from multiple topics.
/// </summary>
public sealed class MergeTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _outputTopic = "";
    private bool _addSourceHeader;

    public override void Start(IDictionary<string, string> config)
    {
        _outputTopic = config.GetValueOrDefault(MergeConfig.OutputTopic, "") ?? "";
        _addSourceHeader = bool.TryParse(config.GetValueOrDefault(MergeConfig.AddSourceHeader, "false"), out var b) && b;
    }

    public override void Stop() { }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (Context.Producer == null)
        {
            return;
        }

        foreach (var record in records)
        {
            try
            {
                Dictionary<string, byte[]>? headers = null;

                if (_addSourceHeader || record.Headers != null)
                {
                    headers = record.Headers != null
                        ? new Dictionary<string, byte[]>(record.Headers)
                        : [];

                    if (_addSourceHeader)
                    {
                        headers["_source_topic"] = System.Text.Encoding.UTF8.GetBytes(record.Topic);
                    }
                }

                await Context.Producer.ProduceAsync(
                    _outputTopic,
                    record.Key,
                    record.Value,
                    headers,
                    cancellationToken);
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(new InvalidOperationException(
                    $"Merge failed for record at offset {record.Offset}: {ex.Message}", ex));
            }
        }
    }
}

/// <summary>
/// Configuration keys for MergeConnector.
/// </summary>
public static class MergeConfig
{
    public const string OutputTopic = "output.topic";
    public const string Topics = "topics";
    public const string AddSourceHeader = "merge.add.source.header";
}
