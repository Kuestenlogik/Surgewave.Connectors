using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Script;

/// <summary>
/// A transform connector that processes records using C# scripts.
/// Consumes from input topics and produces transformed records to output topic.
/// </summary>
[ConnectorMetadata(
    Name = "C# Script Transform",
    Description = "Transforms records using inline C# scripts (Roslyn)",
    Tags = "script, csharp, transform, processor",
    Icon = "Code")]
public sealed class ScriptTransformConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ScriptTransformTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(ScriptConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Input topics to consume from (comma-separated)", EditorHint.Topic)
        .Define(ScriptConnectorConfig.OutputTopic, ConfigType.String, Importance.High,
            "Output topic to write transformed records to", EditorHint.Topic)
        .Define(ScriptConnectorConfig.ScriptPath, ConfigType.String,
            "", Importance.Medium,
            "Path to C# script file", EditorHint.FilePath)
        .Define(ScriptConnectorConfig.ScriptInline, ConfigType.String,
            "", Importance.Medium,
            "Inline C# script (alternative to script.path)", EditorHint.Code, "csharp")
        .Define(ScriptConnectorConfig.ScriptLanguage, ConfigType.String,
            ScriptConnectorConfig.DefaultScriptLanguage, Importance.Low,
            "Script language", EditorHint.Select, options: ["csharp"])
        .Define(ScriptConnectorConfig.TimeoutMs, ConfigType.Int,
            ScriptConnectorConfig.DefaultTimeoutMs, Importance.Low,
            "Script execution timeout in milliseconds")
        .Define(ScriptConnectorConfig.ErrorHandling, ConfigType.String,
            ScriptConnectorConfig.DefaultErrorHandling, Importance.Medium,
            "Error handling strategy", EditorHint.Select, options: ["skip", "fail", "deadletter"])
        .Define(ScriptConnectorConfig.DeadLetterTopic, ConfigType.String,
            "", Importance.Low,
            "Dead letter topic for failed records", EditorHint.Topic)
        .Define(ScriptConnectorConfig.BatchSize, ConfigType.Int,
            ScriptConnectorConfig.DefaultBatchSize, Importance.Low,
            "Number of records per script invocation")
        .Define(ScriptConnectorConfig.ProcessMode, ConfigType.String,
            ScriptConnectorConfig.DefaultProcessMode, Importance.Low,
            "Processing mode", EditorHint.Select, options: ["record", "batch"])
        .Define(ScriptConnectorConfig.Imports, ConfigType.String,
            ScriptConnectorConfig.DefaultImports, Importance.Low,
            "Semicolon-separated list of namespaces to import", EditorHint.Multiline)
        .Define(ScriptConnectorConfig.References, ConfigType.String,
            "", Importance.Low,
            "Semicolon-separated list of assembly references", EditorHint.Multiline);

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(ScriptConnectorConfig.Topics))
            throw new ArgumentException($"Missing required config: {ScriptConnectorConfig.Topics}");
        if (!config.ContainsKey(ScriptConnectorConfig.OutputTopic))
            throw new ArgumentException($"Missing required config: {ScriptConnectorConfig.OutputTopic}");
        if (!config.ContainsKey(ScriptConnectorConfig.ScriptPath) && 
            !config.ContainsKey(ScriptConnectorConfig.ScriptInline))
            throw new ArgumentException($"Missing required config: {ScriptConnectorConfig.ScriptPath} or {ScriptConnectorConfig.ScriptInline}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Script processor is single-threaded to avoid script state issues
        return [new Dictionary<string, string>(_config)];
    }
}
