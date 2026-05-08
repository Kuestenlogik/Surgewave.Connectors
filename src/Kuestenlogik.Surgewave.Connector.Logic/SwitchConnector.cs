using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Logic;

/// <summary>
/// Routes records to different outputs based on a field value.
/// Similar to a switch statement with multiple branches.
/// </summary>
[ConnectorMetadata(
    Name = "Switch",
    Description = "Multi-way branching - route records to different topics based on field value",
    Author = "Surgewave",
    Tags = "logic,branch,switch,route",
    Icon = "alt_route")]
public sealed class SwitchConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SwitchTask);

    private string _fieldPath = "";
    private string _caseMappings = "";
    private string _defaultTopic = "";
    private readonly List<string> _inputTopics = [];

    public override ConfigDef Config => new ConfigDef()
        .Define(SwitchConfig.FieldPath, ConfigType.String, "", Importance.High,
            "JSON path to the field to switch on (e.g., 'type' or 'event.category')")
        .Define(SwitchConfig.Cases, ConfigType.String, "", Importance.High,
            "Case mappings in format 'value1:topic1,value2:topic2,...'", EditorHint.Expression)
        .Define(SwitchConfig.DefaultTopic, ConfigType.String, "", Importance.Medium,
            "Topic for records that don't match any case (optional)", EditorHint.Topic)
        .Define(SwitchConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of input topics", EditorHint.Topic);

    public override void Start(IDictionary<string, string> config)
    {
        _fieldPath = config.GetValueOrDefault(SwitchConfig.FieldPath, "")
            ?? throw new ArgumentException("Field path is required");
        _caseMappings = config.GetValueOrDefault(SwitchConfig.Cases, "")
            ?? throw new ArgumentException("Case mappings are required");
        _defaultTopic = config.GetValueOrDefault(SwitchConfig.DefaultTopic, "") ?? "";

        var topics = config.GetValueOrDefault(SwitchConfig.Topics, "") ?? "";
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
                [SwitchConfig.FieldPath] = _fieldPath,
                [SwitchConfig.Cases] = _caseMappings,
                [SwitchConfig.DefaultTopic] = _defaultTopic,
                [SwitchConfig.Topics] = string.Join(",", _inputTopics)
            }
        ];
    }
}

/// <summary>
/// Task that evaluates switch conditions and routes records.
/// </summary>
public sealed class SwitchTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _fieldPath = "";
    private string _defaultTopic = "";
    private Dictionary<string, string> _caseMap = new();

    public override void Start(IDictionary<string, string> config)
    {
        _fieldPath = config.GetValueOrDefault(SwitchConfig.FieldPath, "") ?? "";
        _defaultTopic = config.GetValueOrDefault(SwitchConfig.DefaultTopic, "") ?? "";

        var cases = config.GetValueOrDefault(SwitchConfig.Cases, "") ?? "";
        _caseMap = ParseCaseMappings(cases);
    }

    private static Dictionary<string, string> ParseCaseMappings(string mappings)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var mapping in mappings.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var parts = mapping.Split(':', 2, StringSplitOptions.TrimEntries);
            if (parts.Length == 2)
            {
                result[parts[0]] = parts[1];
            }
        }

        return result;
    }

    public override void Stop()
    {
        _caseMap.Clear();
    }

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
                string? fieldValue = ExtractFieldValue(record.Value);
                string? targetTopic = null;

                if (fieldValue != null && _caseMap.TryGetValue(fieldValue, out var mappedTopic))
                {
                    targetTopic = mappedTopic;
                }
                else if (!string.IsNullOrEmpty(_defaultTopic))
                {
                    targetTopic = _defaultTopic;
                }

                if (!string.IsNullOrEmpty(targetTopic))
                {
                    await Context.Producer.ProduceAsync(
                        targetTopic,
                        record.Key,
                        record.Value,
                        record.Headers as IDictionary<string, byte[]>,
                        cancellationToken);
                }
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(new InvalidOperationException(
                    $"Switch routing failed for record at offset {record.Offset}: {ex.Message}", ex));
            }
        }
    }

    private string? ExtractFieldValue(byte[] value)
    {
        if (value.Length == 0) return null;

        try
        {
            var json = JsonNode.Parse(value);
            if (json == null) return null;

            // Navigate the path
            var current = json;
            foreach (var segment in _fieldPath.Split('.'))
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

            return current?.GetValue<string>();
        }
        catch
        {
            return null;
        }
    }
}

/// <summary>
/// Configuration keys for SwitchConnector.
/// </summary>
public static class SwitchConfig
{
    public const string FieldPath = "switch.field";
    public const string Cases = "switch.cases";
    public const string DefaultTopic = "switch.default.topic";
    public const string Topics = "topics";
}
