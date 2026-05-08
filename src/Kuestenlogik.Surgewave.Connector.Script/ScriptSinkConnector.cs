using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Script;

/// <summary>
/// A sink connector that processes records using a C# script.
/// The script receives each record via <c>ctx</c> and can perform
/// arbitrary side effects (write files, call APIs, log, etc.).
/// </summary>
[ConnectorMetadata(
    Name = "C# Script Sink",
    Description = "Processes records using inline C# scripts (Roslyn)",
    Tags = "script, csharp, sink, processor",
    Icon = "Code")]
public sealed class ScriptSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ScriptSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(ScriptConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Input topics to consume from (comma-separated)", EditorHint.Topic)
        .Define(ScriptConnectorConfig.ScriptPath, ConfigType.String,
            "", Importance.Medium,
            "Path to C# script file", EditorHint.FilePath)
        .Define(ScriptConnectorConfig.ScriptInline, ConfigType.String,
            "", Importance.Medium,
            "Inline C# script", EditorHint.Code, "csharp")
        .Define(ScriptConnectorConfig.TimeoutMs, ConfigType.Int,
            ScriptConnectorConfig.DefaultTimeoutMs, Importance.Low,
            "Script execution timeout in milliseconds")
        .Define(ScriptConnectorConfig.ErrorHandling, ConfigType.String,
            ScriptConnectorConfig.DefaultErrorHandling, Importance.Medium,
            "Error handling strategy", EditorHint.Select, options: ["skip", "fail", "deadletter"])
        .Define(ScriptConnectorConfig.Imports, ConfigType.String,
            ScriptConnectorConfig.DefaultImports, Importance.Low,
            "Semicolon-separated list of C# using imports", EditorHint.Multiline);

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(ScriptConnectorConfig.Topics))
            throw new ArgumentException($"Missing required config: {ScriptConnectorConfig.Topics}");
        if (!config.ContainsKey(ScriptConnectorConfig.ScriptPath) &&
            !config.ContainsKey(ScriptConnectorConfig.ScriptInline))
            throw new ArgumentException($"Missing required config: {ScriptConnectorConfig.ScriptPath} or {ScriptConnectorConfig.ScriptInline}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
