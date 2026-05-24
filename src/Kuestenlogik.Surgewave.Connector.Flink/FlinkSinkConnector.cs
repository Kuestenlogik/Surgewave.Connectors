using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Flink;

/// <summary>
/// Sink connector that manages Flink jobs via REST API.
/// Supports job submission, cancellation, savepoints, and rescaling.
/// </summary>
[ConnectorMetadata(
    Name = "FlinkSink",
    Description = "Manage Flink jobs via REST API",
    Tags = "flink,sink,job,management")]
[SuppressMessage("Performance", "CA1869:Cache and reuse JsonSerializerOptions", Justification = "Options created once per connector instance")]
public sealed class FlinkSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(FlinkSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(FlinkConnectorConfig.BaseUrl, ConfigType.String, Importance.High,
            "Flink REST API base URL (e.g., http://localhost:8081)")
        .Define(FlinkConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Topics to consume commands from", EditorHint.Topic)
        .Define(FlinkConnectorConfig.TimeoutMs, ConfigType.Int,
            FlinkConnectorConfig.DefaultTimeoutMs, Importance.Medium,
            "HTTP request timeout in milliseconds")
        .Define(FlinkConnectorConfig.ActionType, ConfigType.String,
            FlinkConnectorConfig.ActionTypeSubmit, Importance.Medium,
            "Default action: submit, cancel, savepoint, rescale")
        .Define(FlinkConnectorConfig.JarId, ConfigType.String, "", Importance.Medium,
            "Default JAR ID for job submission")
        .Define(FlinkConnectorConfig.EntryClass, ConfigType.String, "", Importance.Low,
            "Default entry class for job submission")
        .Define(FlinkConnectorConfig.Parallelism, ConfigType.Int,
            FlinkConnectorConfig.DefaultParallelism, Importance.Medium,
            "Default parallelism for job submission")
        .Define(FlinkConnectorConfig.SavepointPath, ConfigType.String, "", Importance.Low,
            "Default savepoint path for restoring jobs")
        .Define(FlinkConnectorConfig.AllowNonRestoredState, ConfigType.Boolean, "false", Importance.Low,
            "Allow non-restored state when restoring from savepoint")
        .Define(FlinkConnectorConfig.AuthType, ConfigType.String,
            FlinkConnectorConfig.AuthTypeNone, Importance.Medium,
            "Authentication type: none, basic, bearer")
        .Define(FlinkConnectorConfig.AuthUsername, ConfigType.String, "", Importance.Medium,
            "Username for basic auth")
        .Define(FlinkConnectorConfig.AuthPassword, ConfigType.Password, "", Importance.Medium,
            "Password for basic auth")
        .Define(FlinkConnectorConfig.AuthToken, ConfigType.Password, "", Importance.Medium,
            "Token for bearer auth");

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(FlinkConnectorConfig.BaseUrl, out var url) || string.IsNullOrEmpty(url))
            throw new ArgumentException($"Missing required config: {FlinkConnectorConfig.BaseUrl}");

        if (!config.TryGetValue(FlinkConnectorConfig.Topics, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Missing required config: {FlinkConnectorConfig.Topics}");
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}

#pragma warning disable CA1812
[SuppressMessage("Performance", "CA1869:Cache and reuse JsonSerializerOptions", Justification = "Options created once per task instance")]
internal sealed class FlinkSinkTask : SinkTask, IDisposable
{
    public override string Version => "1.0.0";

    private FlinkRestClient? _client;
    private string _defaultAction = FlinkConnectorConfig.ActionTypeSubmit;
    private string? _defaultJarId;
    private string? _defaultEntryClass;
    private int _defaultParallelism = 1;
    private string? _defaultSavepointPath;
    private bool _allowNonRestoredState;

    public override void Start(IDictionary<string, string> config)
    {
        var baseUrl = config[FlinkConnectorConfig.BaseUrl];

        var timeoutMs = config.TryGetValue(FlinkConnectorConfig.TimeoutMs, out var t) && int.TryParse(t, out var tv)
            ? tv : FlinkConnectorConfig.DefaultTimeoutMs;

        _defaultAction = config.TryGetValue(FlinkConnectorConfig.ActionType, out var a) ? a : FlinkConnectorConfig.ActionTypeSubmit;
        _defaultJarId = config.TryGetValue(FlinkConnectorConfig.JarId, out var j) ? j : null;
        _defaultEntryClass = config.TryGetValue(FlinkConnectorConfig.EntryClass, out var e) ? e : null;
        _defaultParallelism = config.TryGetValue(FlinkConnectorConfig.Parallelism, out var p) && int.TryParse(p, out var pv)
            ? pv : FlinkConnectorConfig.DefaultParallelism;
        _defaultSavepointPath = config.TryGetValue(FlinkConnectorConfig.SavepointPath, out var s) ? s : null;
        _allowNonRestoredState = config.TryGetValue(FlinkConnectorConfig.AllowNonRestoredState, out var anrs) &&
            bool.TryParse(anrs, out var anrsv) && anrsv;

        var authType = config.TryGetValue(FlinkConnectorConfig.AuthType, out var at) ? at : null;
        var username = config.TryGetValue(FlinkConnectorConfig.AuthUsername, out var u) ? u : null;
        var password = config.TryGetValue(FlinkConnectorConfig.AuthPassword, out var pw) ? pw : null;
        var token = config.TryGetValue(FlinkConnectorConfig.AuthToken, out var tk) ? tk : null;

        _client = new FlinkRestClient(baseUrl, timeoutMs, authType, username, password, token);
    }

    public override void Stop()
    {
        _client?.Dispose();
        _client = null;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            try
            {
                var command = ParseCommand(record);
                await ExecuteCommandAsync(command, cancellationToken);
            }
            catch (Exception ex)
            {
                // Log error but continue processing
                Console.Error.WriteLine($"Flink command failed: {ex.Message}");
            }
        }
    }

    private FlinkCommand ParseCommand(SinkRecord record)
    {
        if (record.Value == null || record.Value.Length == 0)
        {
            return new FlinkCommand { Action = _defaultAction };
        }

        try
        {
            var json = Encoding.UTF8.GetString(record.Value);
            var command = JsonSerializer.Deserialize<FlinkCommand>(json, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            }) ?? new FlinkCommand();

            // Apply defaults
            command.Action ??= _defaultAction;
            command.JarId ??= _defaultJarId;
            command.EntryClass ??= _defaultEntryClass;
            command.Parallelism ??= _defaultParallelism;
            command.SavepointPath ??= _defaultSavepointPath;
            command.AllowNonRestoredState ??= _allowNonRestoredState;

            return command;
        }
        catch
        {
            return new FlinkCommand { Action = _defaultAction };
        }
    }

    private async Task ExecuteCommandAsync(FlinkCommand command, CancellationToken cancellationToken)
    {
        switch (command.Action?.ToLowerInvariant())
        {
            case FlinkConnectorConfig.ActionTypeSubmit:
                await SubmitJobAsync(command, cancellationToken);
                break;

            case FlinkConnectorConfig.ActionTypeCancel:
                if (!string.IsNullOrEmpty(command.JobId))
                    await _client!.CancelJobAsync(command.JobId, cancellationToken);
                break;

            case FlinkConnectorConfig.ActionTypeSavepoint:
                if (!string.IsNullOrEmpty(command.JobId))
                    await _client!.TriggerSavepointAsync(command.JobId, command.TargetDirectory, command.CancelJob ?? false, cancellationToken);
                break;

            case FlinkConnectorConfig.ActionTypeRescale:
                if (!string.IsNullOrEmpty(command.JobId) && command.Parallelism.HasValue)
                    await _client!.RescaleJobAsync(command.JobId, command.Parallelism.Value, cancellationToken);
                break;

            case "stop":
                if (!string.IsNullOrEmpty(command.JobId))
                    await _client!.StopJobWithSavepointAsync(command.JobId, command.TargetDirectory, command.Drain ?? false, cancellationToken);
                break;

            case "upload_jar":
                if (!string.IsNullOrEmpty(command.JarPath))
                    await _client!.UploadJarAsync(command.JarPath, cancellationToken);
                break;

            case "delete_jar":
                if (!string.IsNullOrEmpty(command.JarId))
                    await _client!.DeleteJarAsync(command.JarId, cancellationToken);
                break;
        }
    }

    private async Task SubmitJobAsync(FlinkCommand command, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(command.JarId))
            throw new InvalidOperationException("JAR ID is required for job submission");

        var request = new JarRunRequest
        {
            EntryClass = command.EntryClass,
            Parallelism = command.Parallelism,
            ProgramArgs = command.ProgramArgs,
            SavepointPath = command.SavepointPath,
            AllowNonRestoredState = command.AllowNonRestoredState
        };

        await _client!.RunJarAsync(command.JarId, request, cancellationToken);
    }

    public new void Dispose()
    {
        _client?.Dispose();
        base.Dispose();
    }
}

/// <summary>
/// Command structure for Flink operations.
/// </summary>
public class FlinkCommand
{
    public string? Action { get; set; }
    public string? JobId { get; set; }
    public string? JarId { get; set; }
    public string? JarPath { get; set; }
    public string? EntryClass { get; set; }
    public int? Parallelism { get; set; }
    public string? ProgramArgs { get; set; }
    public string? SavepointPath { get; set; }
    public string? TargetDirectory { get; set; }
    public bool? AllowNonRestoredState { get; set; }
    public bool? CancelJob { get; set; }
    public bool? Drain { get; set; }
}
