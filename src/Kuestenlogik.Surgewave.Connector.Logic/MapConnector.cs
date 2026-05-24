using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;

namespace Kuestenlogik.Surgewave.Connector.Logic;

/// <summary>
/// Transforms records using a C# expression or JSON mapping.
/// </summary>
[ConnectorMetadata(
    Name = "Map",
    Description = "Transform record values using C# expressions or JSON mapping",
    Author = "Surgewave",
    Tags = "logic,transform,map",
    Icon = "transform")]
public sealed class MapConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(MapTask);

    private string _expression = "";
    private string _outputTopic = "";
    private readonly List<string> _inputTopics = [];

    public override ConfigDef Config => new ConfigDef()
        .Define(MapConfig.Expression, ConfigType.String, "", Importance.High,
            "C# expression returning the transformed value (string, byte[], or object for JSON serialization)", EditorHint.Expression)
        .Define(MapConfig.OutputTopic, ConfigType.String, "", Importance.High,
            "Topic to send transformed records to", EditorHint.Topic)
        .Define(MapConfig.Topics, ConfigType.String, "", Importance.High,
            "Comma-separated list of input topics", EditorHint.Topic)
        .Define(MapConfig.KeyExpression, ConfigType.String, "", Importance.Low,
            "Optional C# expression for transforming the key", EditorHint.Expression);

    public override void Start(IDictionary<string, string> config)
    {
        _expression = config.GetValueOrDefault(MapConfig.Expression, "")
            ?? throw new ArgumentException("Map expression is required");
        _outputTopic = config.GetValueOrDefault(MapConfig.OutputTopic, "")
            ?? throw new ArgumentException("Output topic is required");

        var topics = config.GetValueOrDefault(MapConfig.Topics, "") ?? "";
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
                [MapConfig.Expression] = _expression,
                [MapConfig.OutputTopic] = _outputTopic,
                [MapConfig.Topics] = string.Join(",", _inputTopics),
                [MapConfig.KeyExpression] = ""
            }
        ];
    }
}

/// <summary>
/// Task that evaluates map expressions.
/// </summary>
public sealed class MapTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _outputTopic = "";
    private ScriptRunner<object>? _valueRunner;
    private ScriptRunner<object>? _keyRunner;

    public override void Start(IDictionary<string, string> config)
    {
        var expression = config.GetValueOrDefault(MapConfig.Expression, "") ?? "";
        _outputTopic = config.GetValueOrDefault(MapConfig.OutputTopic, "") ?? "";
        var keyExpression = config.GetValueOrDefault(MapConfig.KeyExpression, "") ?? "";

        var options = ScriptOptions.Default
            .AddReferences(
                typeof(JsonDocument).Assembly,
                typeof(JsonNode).Assembly,
                typeof(SinkRecord).Assembly)
            .AddImports(
                "System",
                "System.Linq",
                "System.Text",
                "System.Text.Json",
                "System.Text.Json.Nodes",
                "Kuestenlogik.Surgewave.Connect");

        var valueScript = CSharpScript.Create<object>(expression, options, typeof(MapGlobals));
        _valueRunner = valueScript.CreateDelegate();

        if (!string.IsNullOrWhiteSpace(keyExpression))
        {
            var keyScript = CSharpScript.Create<object>(keyExpression, options, typeof(MapGlobals));
            _keyRunner = keyScript.CreateDelegate();
        }
    }

    public override void Stop()
    {
        _valueRunner = null;
        _keyRunner = null;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (Context.Producer == null || _valueRunner == null)
        {
            return;
        }

        foreach (var record in records)
        {
            try
            {
                JsonNode? json = null;
                if (record.Value.Length > 0)
                {
                    try
                    {
                        json = JsonNode.Parse(record.Value);
                    }
                    catch
                    {
                        // Not valid JSON
                    }
                }

                var globals = new MapGlobals
                {
                    record = record,
                    json = json,
                    key = record.Key != null ? Encoding.UTF8.GetString(record.Key) : null,
                    value = Encoding.UTF8.GetString(record.Value)
                };

                // Transform value
                var transformedValue = await _valueRunner(globals, cancellationToken);
                var outputValue = ConvertToBytes(transformedValue);

                // Transform key if expression provided
                byte[]? outputKey = record.Key;
                if (_keyRunner != null)
                {
                    var transformedKey = await _keyRunner(globals, cancellationToken);
                    outputKey = ConvertToBytes(transformedKey);
                }

                await Context.Producer.ProduceAsync(
                    _outputTopic,
                    outputKey,
                    outputValue,
                    record.Headers as IDictionary<string, byte[]>,
                    cancellationToken);
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(new InvalidOperationException(
                    $"Map expression failed for record at offset {record.Offset}: {ex.Message}", ex));
            }
        }
    }

    private static byte[] ConvertToBytes(object? value)
    {
        return value switch
        {
            null => [],
            byte[] bytes => bytes,
            string str => Encoding.UTF8.GetBytes(str),
            JsonNode node => Encoding.UTF8.GetBytes(node.ToJsonString()),
            _ => Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value))
        };
    }
}

/// <summary>
/// Globals available in map expressions.
/// </summary>
public class MapGlobals
{
    public SinkRecord record { get; set; } = null!;
    public JsonNode? json { get; set; }
    public string? key { get; set; }
    public string value { get; set; } = "";
}

/// <summary>
/// Configuration keys for MapConnector.
/// </summary>
public static class MapConfig
{
    public const string Expression = "map.expression";
    public const string OutputTopic = "output.topic";
    public const string Topics = "topics";
    public const string KeyExpression = "map.key.expression";
}
