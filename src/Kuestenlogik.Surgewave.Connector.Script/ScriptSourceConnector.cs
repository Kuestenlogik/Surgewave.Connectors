using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Script;

/// <summary>
/// A source connector that generates records using a C# script.
/// The script runs in a loop, producing records via <c>result.Emit()</c>.
/// </summary>
[ConnectorMetadata(
    Name = "C# Script Source",
    Description = "Generates records using inline C# scripts (Roslyn)",
    Tags = "script, csharp, source, generator",
    Icon = "Code")]
public sealed class ScriptSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ScriptSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(ScriptConnectorConfig.OutputTopic, ConfigType.String, Importance.High,
            "Output topic to write generated records to", EditorHint.Topic)
        .Define(ScriptConnectorConfig.ScriptPath, ConfigType.String,
            "", Importance.Medium,
            "Path to C# script file", EditorHint.FilePath)
        .Define(ScriptConnectorConfig.ScriptInline, ConfigType.String,
            "", Importance.Medium,
            "Inline C# script", EditorHint.Code, "csharp")
        .Define(ScriptConnectorConfig.TimeoutMs, ConfigType.Int,
            ScriptConnectorConfig.DefaultTimeoutMs, Importance.Low,
            "Script execution timeout in milliseconds")
        .Define(ScriptConnectorConfig.Imports, ConfigType.String,
            ScriptConnectorConfig.DefaultImports, Importance.Low,
            "Semicolon-separated list of C# using imports", EditorHint.Multiline)
        .Define(ScriptSourceConfig.IntervalMs, ConfigType.Int,
            ScriptSourceConfig.DefaultIntervalMs, Importance.Medium,
            "Interval between script invocations in milliseconds");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(ScriptConnectorConfig.OutputTopic))
            throw new ArgumentException($"Missing required config: {ScriptConnectorConfig.OutputTopic}");
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

/// <summary>
/// Additional config keys for script source connectors.
/// </summary>
public static class ScriptSourceConfig
{
    public const string IntervalMs = "interval.ms";
    public const int DefaultIntervalMs = 1000;
}
