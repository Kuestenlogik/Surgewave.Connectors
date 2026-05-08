using System.Reflection;
using System.Text;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Script;

/// <summary>
/// Task that executes C# scripts to transform records.
/// </summary>
public sealed class ScriptTransformTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _outputTopic = "";
    private string _scriptCode = "";
    private int _timeoutMs = ScriptConnectorConfig.DefaultTimeoutMs;
    private string _errorHandling = ScriptConnectorConfig.DefaultErrorHandling;
    private string? _deadLetterTopic;
    private string _processMode = ScriptConnectorConfig.DefaultProcessMode;
    private string[] _imports = [];

    private Script<ScriptResult>? _compiledScript;
    private ScriptRunner<ScriptResult>? _scriptRunner;

    public override void Start(IDictionary<string, string> config)
    {
        _outputTopic = config[ScriptConnectorConfig.OutputTopic];

        if (config.TryGetValue(ScriptConnectorConfig.ScriptPath, out var scriptPath) && !string.IsNullOrEmpty(scriptPath))
        {
            _scriptCode = File.ReadAllText(scriptPath);
        }
        else if (config.TryGetValue(ScriptConnectorConfig.ScriptInline, out var scriptInline))
        {
            _scriptCode = scriptInline;
        }

        if (config.TryGetValue(ScriptConnectorConfig.TimeoutMs, out var timeout))
            _timeoutMs = int.Parse(timeout);
        if (config.TryGetValue(ScriptConnectorConfig.ErrorHandling, out var errorHandling))
            _errorHandling = errorHandling;
        if (config.TryGetValue(ScriptConnectorConfig.DeadLetterTopic, out var dlt))
            _deadLetterTopic = dlt;
        if (config.TryGetValue(ScriptConnectorConfig.ProcessMode, out var processMode))
            _processMode = processMode;
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

        // Async wrapper — scripts can use await
        var wrappedCode = $@"
var result = new ScriptResult();
{_scriptCode}
return result;
";

        _compiledScript = CSharpScript.Create<ScriptResult>(
            wrappedCode,
            options,
            globalsType: typeof(ScriptGlobals));

        // Compile immediately to catch errors early
        _compiledScript.Compile();
        _scriptRunner = _compiledScript.CreateDelegate();
    }

    public override void Stop()
    {
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _scriptRunner == null)
            return;

        foreach (var record in records)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(_timeoutMs);

                var context = new ScriptContext
                {
                    Key = record.Key,
                    Value = record.Value,
                    Timestamp = record.Timestamp,
                    Topic = record.Topic,
                    Partition = record.Partition,
                    Offset = record.Offset,
                    Headers = record.Headers ?? new Dictionary<string, byte[]>()
                };

                var globals = new ScriptGlobals { ctx = context };
                var scriptResult = await _scriptRunner(globals, cts.Token);

                if (scriptResult.Skip)
                    continue;

                if (Context.Producer != null)
                {
                    foreach (var output in scriptResult.Records)
                    {
                        var outputKey = output.UseInputKey ? record.Key : output.Key;
                        var outputValue = output.Value ?? [];
                        var outputTopic = output.Topic ?? _outputTopic;

                        await Context.Producer.ProduceAsync(outputTopic, outputKey, outputValue, cancellationToken);
                    }
                }
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                switch (_errorHandling.ToLowerInvariant())
                {
                    case "fail":
                        throw new InvalidOperationException($"Script execution failed for record at offset {record.Offset}", ex);

                    case "deadletter" when !string.IsNullOrEmpty(_deadLetterTopic) && Context.Producer != null:
                        var errorHeaders = new Dictionary<string, byte[]>
                        {
                            ["__error"] = Encoding.UTF8.GetBytes(ex.Message),
                            ["__error_type"] = Encoding.UTF8.GetBytes(ex.GetType().Name)
                        };
                        await Context.Producer.ProduceAsync(_deadLetterTopic, record.Key, record.Value, errorHeaders, cancellationToken);
                        break;

                    case "skip":
                    default:
                        // Just skip the record
                        break;
                }
            }
        }
    }
}

/// <summary>
/// Global variables available to the script.
/// </summary>
public sealed class ScriptGlobals
{
    public ScriptContext ctx { get; init; } = null!;
}
