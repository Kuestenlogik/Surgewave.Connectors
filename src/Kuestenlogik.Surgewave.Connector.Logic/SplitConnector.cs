using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Logic;

/// <summary>
/// Splits array records into individual records.
/// Each array element becomes a separate output record.
/// </summary>
[ConnectorMetadata(
    Name = "Split",
    Description = "Split arrays into individual records",
    Author = "Surgewave",
    Tags = "logic,split,array,explode",
    Icon = "view_stream")]
public sealed class SplitConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SplitTask);

    private string _arrayPath = "";
    private string _outputTopic = "";
    private readonly List<string> _inputTopics = [];

    public override ConfigDef Config => new ConfigDef()
        .Define(SplitConfig.ArrayPath, ConfigType.String, "", Importance.High,
            "JSON path to the array field (e.g., 'items' or 'data.records'). Use '.' for root array.")
        .Define(SplitConfig.OutputTopic, ConfigType.String, "", Importance.High,
            "Topic to send split records to", EditorHint.Topic)
        .Define(SplitConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of input topics", EditorHint.Topic)
        .Define(SplitConfig.IncludeIndex, ConfigType.Boolean, "false", Importance.Low,
            "Include array index as '_index' header");

    public override void Start(IDictionary<string, string> config)
    {
        _arrayPath = config.GetValueOrDefault(SplitConfig.ArrayPath, "") ?? ".";
        _outputTopic = config.GetValueOrDefault(SplitConfig.OutputTopic, "")
            ?? throw new ArgumentException("Output topic is required");

        var topics = config.GetValueOrDefault(SplitConfig.Topics, "") ?? "";
        _inputTopics.AddRange(topics.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));

        if (_inputTopics.Count == 0)
        {
            throw new ArgumentException("At least one input topic is required");
        }
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return
        [
            new Dictionary<string, string>
            {
                [SplitConfig.ArrayPath] = _arrayPath,
                [SplitConfig.OutputTopic] = _outputTopic,
                [SplitConfig.Topics] = string.Join(",", _inputTopics),
                [SplitConfig.IncludeIndex] = "false"
            }
        ];
    }
}

/// <summary>
/// Task that splits arrays into individual records.
/// </summary>
public sealed class SplitTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _arrayPath = "";
    private string _outputTopic = "";
    private bool _includeIndex;

    public override void Start(IDictionary<string, string> config)
    {
        _arrayPath = config.GetValueOrDefault(SplitConfig.ArrayPath, "") ?? ".";
        _outputTopic = config.GetValueOrDefault(SplitConfig.OutputTopic, "") ?? "";
        _includeIndex = bool.TryParse(config.GetValueOrDefault(SplitConfig.IncludeIndex, "false"), out var b) && b;
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
                var array = ExtractArray(record.Value);
                if (array == null)
                {
                    // Not an array, pass through unchanged
                    await Context.Producer.ProduceAsync(
                        _outputTopic,
                        record.Key,
                        record.Value,
                        record.Headers as IDictionary<string, byte[]>,
                        cancellationToken);
                    continue;
                }

                var index = 0;
                foreach (var element in array)
                {
                    var elementBytes = Encoding.UTF8.GetBytes(element?.ToJsonString() ?? "null");

                    Dictionary<string, byte[]>? headers = null;
                    if (_includeIndex || record.Headers != null)
                    {
                        headers = record.Headers != null
                            ? new Dictionary<string, byte[]>(record.Headers)
                            : [];

                        if (_includeIndex)
                        {
                            headers["_index"] = BitConverter.GetBytes(index);
                        }
                    }

                    await Context.Producer.ProduceAsync(
                        _outputTopic,
                        record.Key,
                        elementBytes,
                        headers,
                        cancellationToken);

                    index++;
                }
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(new InvalidOperationException(
                    $"Split failed for record at offset {record.Offset}: {ex.Message}", ex));
            }
        }
    }

    private JsonArray? ExtractArray(byte[] value)
    {
        if (value.Length == 0) return null;

        try
        {
            var json = JsonNode.Parse(value);
            if (json == null) return null;

            // Root array
            if (_arrayPath == "." || string.IsNullOrEmpty(_arrayPath))
            {
                return json as JsonArray;
            }

            // Navigate the path
            var current = json;
            foreach (var segment in _arrayPath.Split('.'))
            {
                if (current is JsonObject obj && obj.TryGetPropertyValue(segment, out var next))
                {
                    current = next;
                }
                else
                {
                    return null;
                }
            }

            return current as JsonArray;
        }
        catch
        {
            return null;
        }
    }
}

/// <summary>
/// Configuration keys for SplitConnector.
/// </summary>
public static class SplitConfig
{
    public const string ArrayPath = "split.array.path";
    public const string OutputTopic = "output.topic";
    public const string Topics = "topics";
    public const string IncludeIndex = "split.include.index";
}
