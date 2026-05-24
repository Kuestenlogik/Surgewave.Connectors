using System.Reflection;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Script;

/// <summary>
/// Source task that executes a C# script in a loop to generate records.
/// The script accesses a <see cref="SourceScriptContext"/> via <c>ctx</c> and
/// emits records via <c>result.Emit()</c>.
/// </summary>
public sealed class ScriptSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _outputTopic = "";
    private string _scriptCode = "";
    private int _timeoutMs = ScriptConnectorConfig.DefaultTimeoutMs;
    private int _intervalMs = ScriptSourceConfig.DefaultIntervalMs;
    private string[] _imports = [];

    private ScriptRunner<ScriptResult>? _scriptRunner;
    private long _invocationCount;

    public override void Start(IDictionary<string, string> config)
    {
        _outputTopic = config[ScriptConnectorConfig.OutputTopic];

        if (config.TryGetValue(ScriptConnectorConfig.ScriptPath, out var path) && !string.IsNullOrEmpty(path))
            _scriptCode = File.ReadAllText(path);
        else if (config.TryGetValue(ScriptConnectorConfig.ScriptInline, out var inline))
            _scriptCode = inline;

        if (config.TryGetValue(ScriptConnectorConfig.TimeoutMs, out var timeout))
            _timeoutMs = int.Parse(timeout);
        if (config.TryGetValue(ScriptSourceConfig.IntervalMs, out var interval))
            _intervalMs = int.Parse(interval);
        if (config.TryGetValue(ScriptConnectorConfig.Imports, out var imports))
            _imports = imports.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        else
            _imports = ScriptConnectorConfig.DefaultImports.Split(';');

        CompileScript();
    }

    private void CompileScript()
    {
        var options = ScriptOptions.Default
            .WithReferences(
                typeof(object).Assembly,
                typeof(Enumerable).Assembly,
                typeof(System.Text.Json.JsonSerializer).Assembly,
                typeof(System.Net.Http.HttpClient).Assembly,
                Assembly.GetExecutingAssembly())
            .WithImports([.. _imports, "System.Threading.Tasks", "System.Net.Http"]);

        var wrappedCode = $@"
var result = new ScriptResult();
{_scriptCode}
return result;
";

        var script = CSharpScript.Create<ScriptResult>(
            wrappedCode, options, globalsType: typeof(SourceScriptGlobals));
        script.Compile();
        _scriptRunner = script.CreateDelegate();
    }

    public override void Stop() { }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_scriptRunner == null)
            return [];

        await Task.Delay(_intervalMs, cancellationToken);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_timeoutMs);

        _invocationCount++;

        var ctx = new SourceScriptContext
        {
            InvocationCount = _invocationCount,
            Timestamp = DateTimeOffset.UtcNow
        };

        var globals = new SourceScriptGlobals { ctx = ctx };
        var result = await _scriptRunner(globals, cts.Token);

        var records = new List<SourceRecord>();
        foreach (var output in result.Records)
        {
            records.Add(new SourceRecord
            {
                Topic = output.Topic ?? _outputTopic,
                Key = output.Key,
                Value = output.Value ?? [],
                Timestamp = DateTimeOffset.UtcNow,
                Headers = output.Headers,
                SourcePartition = new Dictionary<string, object> { ["script"] = "true" },
                SourceOffset = new Dictionary<string, object> { ["invocation"] = _invocationCount }
            });
        }

        return records;
    }
}

/// <summary>
/// Context available to source scripts.
/// </summary>
public sealed class SourceScriptContext
{
    /// <summary>How many times the script has been invoked.</summary>
    public long InvocationCount { get; init; }

    /// <summary>Current timestamp.</summary>
    public DateTimeOffset Timestamp { get; init; }
}

/// <summary>
/// Globals for source scripts.
/// </summary>
public sealed class SourceScriptGlobals
{
    public SourceScriptContext ctx { get; init; } = null!;
}
