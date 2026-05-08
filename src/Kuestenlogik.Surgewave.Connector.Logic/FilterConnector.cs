using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;

namespace Kuestenlogik.Surgewave.Connector.Logic;

/// <summary>
/// Filters records based on a C# expression.
/// Records matching the condition are passed through, others are dropped.
/// </summary>
[ConnectorMetadata(
    Name = "Filter",
    Description = "Filter records based on a C# expression condition",
    Author = "Surgewave",
    Tags = "logic,filter,transform",
    Icon = "filter_alt")]
public sealed class FilterConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(FilterTask);

    private string _condition = "";
    private string _outputTopic = "";
    private readonly List<string> _inputTopics = [];

    public override ConfigDef Config => new ConfigDef()
        .Define(FilterConfig.Condition, ConfigType.String, "", Importance.High,
            "C# expression that evaluates to bool. Use 'record' for SinkRecord, 'json' for parsed JSON.", EditorHint.Condition)
        .Define(FilterConfig.OutputTopic, ConfigType.String, "", Importance.High,
            "Topic to send matching records to", EditorHint.Topic)
        .Define(FilterConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of input topics", EditorHint.Topic);

    public override void Start(IDictionary<string, string> config)
    {
        _condition = config.GetValueOrDefault(FilterConfig.Condition, "")
            ?? throw new ArgumentException("Filter condition is required");
        _outputTopic = config.GetValueOrDefault(FilterConfig.OutputTopic, "")
            ?? throw new ArgumentException("Output topic is required");

        var topics = config.GetValueOrDefault(FilterConfig.Topics, "") ?? "";
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
                [FilterConfig.Condition] = _condition,
                [FilterConfig.OutputTopic] = _outputTopic,
                [FilterConfig.Topics] = string.Join(",", _inputTopics)
            }
        ];
    }
}

/// <summary>
/// Task that evaluates filter conditions.
/// </summary>
public sealed class FilterTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _condition = "";
    private string _outputTopic = "";
    private Script<bool>? _compiledScript;
    private ScriptRunner<bool>? _scriptRunner;

    public override void Start(IDictionary<string, string> config)
    {
        _condition = config.GetValueOrDefault(FilterConfig.Condition, "") ?? "";
        _outputTopic = config.GetValueOrDefault(FilterConfig.OutputTopic, "") ?? "";

        // Compile the condition script
        var options = ScriptOptions.Default
            .AddReferences(typeof(JsonDocument).Assembly, typeof(SinkRecord).Assembly)
            .AddImports("System", "System.Linq", "System.Text", "System.Text.Json", "Kuestenlogik.Surgewave.Connect");

        _compiledScript = CSharpScript.Create<bool>(_condition, options, typeof(FilterGlobals));
        _scriptRunner = _compiledScript.CreateDelegate();
    }

    public override void Stop()
    {
        _compiledScript = null;
        _scriptRunner = null;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (Context.Producer == null || _scriptRunner == null)
        {
            return;
        }

        foreach (var record in records)
        {
            try
            {
                JsonDocument? json = null;
                if (record.Value.Length > 0)
                {
                    try
                    {
                        json = JsonDocument.Parse(record.Value);
                    }
                    catch
                    {
                        // Not valid JSON, leave null
                    }
                }

                var globals = new FilterGlobals
                {
                    record = record,
                    json = json,
                    key = record.Key != null ? System.Text.Encoding.UTF8.GetString(record.Key) : null,
                    value = System.Text.Encoding.UTF8.GetString(record.Value)
                };

                var match = await _scriptRunner(globals, cancellationToken);

                if (match)
                {
                    await Context.Producer.ProduceAsync(
                        _outputTopic,
                        record.Key,
                        record.Value,
                        record.Headers as IDictionary<string, byte[]>,
                        cancellationToken);
                }
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(new InvalidOperationException(
                    $"Filter condition failed for record at offset {record.Offset}: {ex.Message}", ex));
            }
        }
    }
}

/// <summary>
/// Globals available in filter expressions.
/// </summary>
public class FilterGlobals
{
    public SinkRecord record { get; set; } = null!;
    public JsonDocument? json { get; set; }
    public string? key { get; set; }
    public string value { get; set; } = "";
}

/// <summary>
/// Configuration keys for FilterConnector.
/// </summary>
public static class FilterConfig
{
    public const string Condition = "filter.condition";
    public const string OutputTopic = "output.topic";
    public const string Topics = "topics";
}
