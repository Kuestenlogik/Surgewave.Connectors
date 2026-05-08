using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;

namespace Kuestenlogik.Surgewave.Connector.Logic;

/// <summary>
/// Routes records to different outputs based on a condition.
/// True branch goes to one topic, false branch to another.
/// </summary>
[ConnectorMetadata(
    Name = "If",
    Description = "Conditional branching - route records based on a condition",
    Author = "Surgewave",
    Tags = "logic,branch,if,condition",
    Icon = "call_split")]
public sealed class IfConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(IfTask);

    private string _condition = "";
    private string _trueTopic = "";
    private string _falseTopic = "";
    private readonly List<string> _inputTopics = [];

    public override ConfigDef Config => new ConfigDef()
        .Define(IfConfig.Condition, ConfigType.String, "", Importance.High,
            "C# expression that evaluates to bool", EditorHint.Condition)
        .Define(IfConfig.TrueTopic, ConfigType.String, "", Importance.High,
            "Topic to send records when condition is true", EditorHint.Topic)
        .Define(IfConfig.FalseTopic, ConfigType.String, "", Importance.Medium,
            "Topic to send records when condition is false (optional, records are dropped if not specified)", EditorHint.Topic)
        .Define(IfConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of input topics", EditorHint.Topic);

    public override void Start(IDictionary<string, string> config)
    {
        _condition = config.GetValueOrDefault(IfConfig.Condition, "")
            ?? throw new ArgumentException("Condition is required");
        _trueTopic = config.GetValueOrDefault(IfConfig.TrueTopic, "")
            ?? throw new ArgumentException("True topic is required");
        _falseTopic = config.GetValueOrDefault(IfConfig.FalseTopic, "") ?? "";

        var topics = config.GetValueOrDefault(IfConfig.Topics, "") ?? "";
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
                [IfConfig.Condition] = _condition,
                [IfConfig.TrueTopic] = _trueTopic,
                [IfConfig.FalseTopic] = _falseTopic,
                [IfConfig.Topics] = string.Join(",", _inputTopics)
            }
        ];
    }
}

/// <summary>
/// Task that evaluates if conditions and routes records.
/// </summary>
public sealed class IfTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _trueTopic = "";
    private string _falseTopic = "";
    private ScriptRunner<bool>? _scriptRunner;

    public override void Start(IDictionary<string, string> config)
    {
        var condition = config.GetValueOrDefault(IfConfig.Condition, "") ?? "";
        _trueTopic = config.GetValueOrDefault(IfConfig.TrueTopic, "") ?? "";
        _falseTopic = config.GetValueOrDefault(IfConfig.FalseTopic, "") ?? "";

        var options = ScriptOptions.Default
            .AddReferences(typeof(JsonDocument).Assembly, typeof(SinkRecord).Assembly)
            .AddImports("System", "System.Linq", "System.Text", "System.Text.Json", "Kuestenlogik.Surgewave.Connect");

        var script = CSharpScript.Create<bool>(condition, options, typeof(FilterGlobals));
        _scriptRunner = script.CreateDelegate();
    }

    public override void Stop()
    {
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
                        // Not valid JSON
                    }
                }

                var globals = new FilterGlobals
                {
                    record = record,
                    json = json,
                    key = record.Key != null ? System.Text.Encoding.UTF8.GetString(record.Key) : null,
                    value = System.Text.Encoding.UTF8.GetString(record.Value)
                };

                var result = await _scriptRunner(globals, cancellationToken);

                var targetTopic = result ? _trueTopic : _falseTopic;

                // Only produce if there's a target topic
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
                    $"If condition failed for record at offset {record.Offset}: {ex.Message}", ex));
            }
        }
    }
}

/// <summary>
/// Configuration keys for IfConnector.
/// </summary>
public static class IfConfig
{
    public const string Condition = "if.condition";
    public const string TrueTopic = "if.true.topic";
    public const string FalseTopic = "if.false.topic";
    public const string Topics = "topics";
}
