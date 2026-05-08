using System.Reflection;
using System.Text;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Script;

/// <summary>
/// Sink task that executes a C# script for each record.
/// Unlike the transform task, this does NOT produce output records —
/// the script handles the record (write to file, call API, log, etc.).
/// </summary>
public sealed class ScriptSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _scriptCode = "";
    private int _timeoutMs = ScriptConnectorConfig.DefaultTimeoutMs;
    private string _errorHandling = ScriptConnectorConfig.DefaultErrorHandling;
    private string[] _imports = [];

    private ScriptRunner<object?>? _scriptRunner;

    public override void Start(IDictionary<string, string> config)
    {
        if (config.TryGetValue(ScriptConnectorConfig.ScriptPath, out var path) && !string.IsNullOrEmpty(path))
            _scriptCode = File.ReadAllText(path);
        else if (config.TryGetValue(ScriptConnectorConfig.ScriptInline, out var inline))
            _scriptCode = inline;

        if (config.TryGetValue(ScriptConnectorConfig.TimeoutMs, out var timeout))
            _timeoutMs = int.Parse(timeout);
        if (config.TryGetValue(ScriptConnectorConfig.ErrorHandling, out var errorHandling))
            _errorHandling = errorHandling;
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
                typeof(Console).Assembly,
                typeof(System.Net.Http.HttpClient).Assembly,
                Assembly.GetExecutingAssembly())
            .WithImports([.. _imports, "System.Threading.Tasks", "System.Net.Http"]);

        var script = CSharpScript.Create<object?>(
            _scriptCode, options, globalsType: typeof(ScriptGlobals));
        script.Compile();
        _scriptRunner = script.CreateDelegate();
    }

    public override void Stop() { }

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
                await _scriptRunner(globals, cts.Token);
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                if (_errorHandling.Equals("fail", StringComparison.OrdinalIgnoreCase))
                    throw new InvalidOperationException(
                        $"Script sink failed for record at offset {record.Offset}", ex);
            }
        }
    }
}
