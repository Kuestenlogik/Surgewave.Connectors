using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Spark;

/// <summary>
/// Sink connector that manages Spark jobs via REST API and Livy.
/// Supports job submission, cancellation, and interactive sessions.
/// </summary>
[ConnectorMetadata(
    Name = "SparkSink",
    Description = "Manage Spark jobs via REST API and Livy",
    Tags = "spark,livy,sink,job,management")]
[SuppressMessage("Performance", "CA1869:Cache and reuse JsonSerializerOptions", Justification = "Options created once per connector instance")]
public sealed class SparkSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SparkSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(SparkConnectorConfig.BaseUrl, ConfigType.String, "", Importance.Medium,
            "Spark Master REST API URL")
        .Define(SparkConnectorConfig.LivyUrl, ConfigType.String, "", Importance.Medium,
            "Livy REST API URL")
        .Define(SparkConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Topics to consume commands from", EditorHint.Topic)
        .Define(SparkConnectorConfig.TimeoutMs, ConfigType.Int,
            SparkConnectorConfig.DefaultTimeoutMs, Importance.Medium,
            "HTTP request timeout in milliseconds")
        .Define(SparkConnectorConfig.ActionType, ConfigType.String,
            SparkConnectorConfig.ActionTypeSubmit, Importance.Medium,
            "Default action: submit, kill, statement, create_session, delete_session")
        .Define(SparkConnectorConfig.ApiMode, ConfigType.String,
            SparkConnectorConfig.ApiModeLivy, Importance.Medium,
            "API mode: spark or livy")
        // Livy session defaults
        .Define(SparkConnectorConfig.SessionKind, ConfigType.String,
            SparkConnectorConfig.SessionKindSpark, Importance.Medium,
            "Session kind: spark, pyspark, sparkr, sql")
        .Define(SparkConnectorConfig.DriverMemory, ConfigType.String,
            SparkConnectorConfig.DefaultDriverMemory, Importance.Low,
            "Driver memory (e.g., 1g, 512m)")
        .Define(SparkConnectorConfig.DriverCores, ConfigType.Int,
            SparkConnectorConfig.DefaultDriverCores, Importance.Low,
            "Number of driver cores")
        .Define(SparkConnectorConfig.ExecutorMemory, ConfigType.String,
            SparkConnectorConfig.DefaultExecutorMemory, Importance.Low,
            "Executor memory")
        .Define(SparkConnectorConfig.ExecutorCores, ConfigType.Int,
            SparkConnectorConfig.DefaultExecutorCores, Importance.Low,
            "Number of executor cores")
        .Define(SparkConnectorConfig.NumExecutors, ConfigType.Int,
            SparkConnectorConfig.DefaultNumExecutors, Importance.Low,
            "Number of executors")
        // Auth
        .Define(SparkConnectorConfig.AuthType, ConfigType.String,
            SparkConnectorConfig.AuthTypeNone, Importance.Medium,
            "Authentication type: none, basic")
        .Define(SparkConnectorConfig.AuthUsername, ConfigType.String, "", Importance.Medium,
            "Username for basic auth")
        .Define(SparkConnectorConfig.AuthPassword, ConfigType.Password, "", Importance.Medium,
            "Password for basic auth");

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        var hasSparkUrl = config.TryGetValue(SparkConnectorConfig.BaseUrl, out var sparkUrl) && !string.IsNullOrEmpty(sparkUrl);
        var hasLivyUrl = config.TryGetValue(SparkConnectorConfig.LivyUrl, out var livyUrl) && !string.IsNullOrEmpty(livyUrl);

        if (!hasSparkUrl && !hasLivyUrl)
            throw new ArgumentException($"At least one of {SparkConnectorConfig.BaseUrl} or {SparkConnectorConfig.LivyUrl} is required");

        if (!config.TryGetValue(SparkConnectorConfig.Topics, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Missing required config: {SparkConnectorConfig.Topics}");
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}

#pragma warning disable CA1812
[SuppressMessage("Performance", "CA1869:Cache and reuse JsonSerializerOptions", Justification = "Options created once per task instance")]
internal sealed class SparkSinkTask : SinkTask, IDisposable
{
    public override string Version => "1.0.0";

    private SparkRestClient? _client;
    private string _defaultAction = SparkConnectorConfig.ActionTypeSubmit;
    private string _apiMode = SparkConnectorConfig.ApiModeLivy;
    private string _sessionKind = SparkConnectorConfig.SessionKindSpark;
    private string _driverMemory = SparkConnectorConfig.DefaultDriverMemory;
    private int _driverCores = SparkConnectorConfig.DefaultDriverCores;
    private string _executorMemory = SparkConnectorConfig.DefaultExecutorMemory;
    private int _executorCores = SparkConnectorConfig.DefaultExecutorCores;
    private int _numExecutors = SparkConnectorConfig.DefaultNumExecutors;

    public override void Start(IDictionary<string, string> config)
    {
        var sparkUrl = config.TryGetValue(SparkConnectorConfig.BaseUrl, out var su) ? su : null;
        var livyUrl = config.TryGetValue(SparkConnectorConfig.LivyUrl, out var lu) ? lu : null;

        var timeoutMs = config.TryGetValue(SparkConnectorConfig.TimeoutMs, out var t) && int.TryParse(t, out var tv)
            ? tv : SparkConnectorConfig.DefaultTimeoutMs;

        _defaultAction = config.TryGetValue(SparkConnectorConfig.ActionType, out var a) ? a : SparkConnectorConfig.ActionTypeSubmit;
        _apiMode = config.TryGetValue(SparkConnectorConfig.ApiMode, out var m) ? m : SparkConnectorConfig.ApiModeLivy;
        _sessionKind = config.TryGetValue(SparkConnectorConfig.SessionKind, out var k) ? k : SparkConnectorConfig.SessionKindSpark;
        _driverMemory = config.TryGetValue(SparkConnectorConfig.DriverMemory, out var dm) ? dm : SparkConnectorConfig.DefaultDriverMemory;
        _driverCores = config.TryGetValue(SparkConnectorConfig.DriverCores, out var dc) && int.TryParse(dc, out var dcv) ? dcv : SparkConnectorConfig.DefaultDriverCores;
        _executorMemory = config.TryGetValue(SparkConnectorConfig.ExecutorMemory, out var em) ? em : SparkConnectorConfig.DefaultExecutorMemory;
        _executorCores = config.TryGetValue(SparkConnectorConfig.ExecutorCores, out var ec) && int.TryParse(ec, out var ecv) ? ecv : SparkConnectorConfig.DefaultExecutorCores;
        _numExecutors = config.TryGetValue(SparkConnectorConfig.NumExecutors, out var ne) && int.TryParse(ne, out var nev) ? nev : SparkConnectorConfig.DefaultNumExecutors;

        var authType = config.TryGetValue(SparkConnectorConfig.AuthType, out var at) ? at : null;
        var username = config.TryGetValue(SparkConnectorConfig.AuthUsername, out var u) ? u : null;
        var password = config.TryGetValue(SparkConnectorConfig.AuthPassword, out var pw) ? pw : null;

        _client = new SparkRestClient(sparkUrl, livyUrl, timeoutMs, authType, username, password);
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
                Console.Error.WriteLine($"Spark command failed: {ex.Message}");
            }
        }
    }

    private SparkCommand ParseCommand(SinkRecord record)
    {
        if (record.Value == null || record.Value.Length == 0)
        {
            return new SparkCommand { Action = _defaultAction };
        }

        try
        {
            var json = Encoding.UTF8.GetString(record.Value);
            var command = JsonSerializer.Deserialize<SparkCommand>(json, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            }) ?? new SparkCommand();

            // Apply defaults
            command.Action ??= _defaultAction;
            command.ApiMode ??= _apiMode;
            command.Kind ??= _sessionKind;
            command.DriverMemory ??= _driverMemory;
            command.DriverCores ??= _driverCores;
            command.ExecutorMemory ??= _executorMemory;
            command.ExecutorCores ??= _executorCores;
            command.NumExecutors ??= _numExecutors;

            return command;
        }
        catch
        {
            return new SparkCommand { Action = _defaultAction };
        }
    }

    private async Task ExecuteCommandAsync(SparkCommand command, CancellationToken ct)
    {
        var useLivy = command.ApiMode?.Equals(SparkConnectorConfig.ApiModeLivy, StringComparison.OrdinalIgnoreCase) != false;

        switch (command.Action?.ToLowerInvariant())
        {
            case SparkConnectorConfig.ActionTypeSubmit:
                if (useLivy)
                    await SubmitLivyBatchAsync(command, ct);
                else
                    await SubmitSparkJobAsync(command, ct);
                break;

            case SparkConnectorConfig.ActionTypeKill:
                if (useLivy && command.BatchId.HasValue)
                    await _client!.DeleteBatchAsync(command.BatchId.Value, ct);
                else if (!useLivy && !string.IsNullOrEmpty(command.SubmissionId))
                    await _client!.KillApplicationAsync(command.SubmissionId, ct);
                break;

            case SparkConnectorConfig.ActionTypeCreateSession:
                await CreateLivySessionAsync(command, ct);
                break;

            case SparkConnectorConfig.ActionTypeDeleteSession:
                if (command.SessionId.HasValue)
                    await _client!.DeleteSessionAsync(command.SessionId.Value, ct);
                break;

            case SparkConnectorConfig.ActionTypeStatement:
                if (command.SessionId.HasValue && !string.IsNullOrEmpty(command.Code))
                    await _client!.ExecuteStatementAsync(command.SessionId.Value, command.Code, command.Kind, ct);
                break;

            case "cancel_statement":
                if (command.SessionId.HasValue && command.StatementId.HasValue)
                    await _client!.CancelStatementAsync(command.SessionId.Value, command.StatementId.Value, ct);
                break;
        }
    }

    private async Task SubmitLivyBatchAsync(SparkCommand command, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(command.File))
            throw new InvalidOperationException("File is required for Livy batch submission");

        var request = new LivyBatchRequest
        {
            File = command.File,
            ClassName = command.MainClass,
            Args = command.Args,
            Jars = command.Jars,
            PyFiles = command.PyFiles,
            Files = command.Files,
            DriverMemory = command.DriverMemory,
            DriverCores = command.DriverCores,
            ExecutorMemory = command.ExecutorMemory,
            ExecutorCores = command.ExecutorCores,
            NumExecutors = command.NumExecutors,
            Archives = command.Archives,
            Queue = command.Queue,
            Name = command.Name,
            Conf = command.Conf
        };

        await _client!.SubmitBatchAsync(request, ct);
    }

    private async Task SubmitSparkJobAsync(SparkCommand command, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(command.File))
            throw new InvalidOperationException("File (appResource) is required for Spark submission");

        var request = new SparkSubmissionRequest
        {
            AppResource = command.File,
            MainClass = command.MainClass,
            AppArgs = command.Args,
            SparkProperties = command.Conf
        };

        await _client!.SubmitApplicationAsync(request, ct);
    }

    private async Task CreateLivySessionAsync(SparkCommand command, CancellationToken ct)
    {
        var request = new LivySessionRequest
        {
            Kind = command.Kind,
            Jars = command.Jars,
            PyFiles = command.PyFiles,
            Files = command.Files,
            DriverMemory = command.DriverMemory,
            DriverCores = command.DriverCores,
            ExecutorMemory = command.ExecutorMemory,
            ExecutorCores = command.ExecutorCores,
            NumExecutors = command.NumExecutors,
            Archives = command.Archives,
            Queue = command.Queue,
            Name = command.Name,
            Conf = command.Conf
        };

        await _client!.CreateSessionAsync(request, ct);
    }

    public new void Dispose()
    {
        _client?.Dispose();
        base.Dispose();
    }
}

/// <summary>
/// Command structure for Spark/Livy operations.
/// </summary>
public class SparkCommand
{
    public string? Action { get; set; }
    public string? ApiMode { get; set; }

    // Job/Batch submission
    public string? File { get; set; }
    public string? MainClass { get; set; }
    public List<string>? Args { get; set; }
    public List<string>? Jars { get; set; }
    public List<string>? PyFiles { get; set; }
    public List<string>? Files { get; set; }
    public List<string>? Archives { get; set; }
    public string? Queue { get; set; }
    public string? Name { get; set; }
    public Dictionary<string, string>? Conf { get; set; }

    // Resource configuration
    public string? DriverMemory { get; set; }
    public int? DriverCores { get; set; }
    public string? ExecutorMemory { get; set; }
    public int? ExecutorCores { get; set; }
    public int? NumExecutors { get; set; }

    // Session/Statement
    public string? Kind { get; set; }
    public int? SessionId { get; set; }
    public int? StatementId { get; set; }
    public string? Code { get; set; }

    // Batch/Submission
    public int? BatchId { get; set; }
    public string? SubmissionId { get; set; }
}
