using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Spark;

/// <summary>
/// Source connector that polls Spark/Livy REST API for cluster status, job metrics, and events.
/// Emits monitoring data to Surgewave topics for observability and alerting.
/// </summary>
[ConnectorMetadata(
    Name = "SparkSource",
    Description = "Monitor Spark cluster and job metrics",
    Tags = "spark,livy,source,monitoring,metrics")]
public sealed class SparkSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SparkSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(SparkConnectorConfig.BaseUrl, ConfigType.String, "", Importance.Medium,
            "Spark Master REST API URL (e.g., http://localhost:8080)")
        .Define(SparkConnectorConfig.LivyUrl, ConfigType.String, "", Importance.Medium,
            "Livy REST API URL (e.g., http://localhost:8998)")
        .Define(SparkConnectorConfig.OutputTopic, ConfigType.String, Importance.High,
            "Topic to emit Spark metrics and events", EditorHint.Topic)
        .Define(SparkConnectorConfig.PollIntervalMs, ConfigType.Int,
            SparkConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Polling interval in milliseconds")
        .Define(SparkConnectorConfig.TimeoutMs, ConfigType.Int,
            SparkConnectorConfig.DefaultTimeoutMs, Importance.Medium,
            "HTTP request timeout in milliseconds")
        .Define(SparkConnectorConfig.IncludeApplicationMetrics, ConfigType.Boolean, "true", Importance.Medium,
            "Include application-level metrics")
        .Define(SparkConnectorConfig.IncludeExecutorMetrics, ConfigType.Boolean, "false", Importance.Low,
            "Include executor-level metrics")
        .Define(SparkConnectorConfig.IncludeStageMetrics, ConfigType.Boolean, "false", Importance.Low,
            "Include stage-level metrics")
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

        if (!config.TryGetValue(SparkConnectorConfig.OutputTopic, out var topic) || string.IsNullOrEmpty(topic))
            throw new ArgumentException($"Missing required config: {SparkConnectorConfig.OutputTopic}");
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}

#pragma warning disable CA1812
internal sealed class SparkSourceTask : SourceTask, IDisposable
{
    public override string Version => "1.0.0";

    private SparkRestClient? _client;
    private string _topic = "";
    private int _pollIntervalMs;
    private bool _includeAppMetrics;
    private bool _includeExecutorMetrics;
    private bool _includeStageMetrics;
    private bool _hasSparkUrl;
    private bool _hasLivyUrl;
    private DateTimeOffset _lastPoll = DateTimeOffset.MinValue;
    private readonly Dictionary<string, string> _lastAppStates = new();
    private readonly Dictionary<int, string> _lastSessionStates = new();
    private readonly Dictionary<int, string> _lastBatchStates = new();

    public override void Start(IDictionary<string, string> config)
    {
        var sparkUrl = config.TryGetValue(SparkConnectorConfig.BaseUrl, out var su) ? su : null;
        var livyUrl = config.TryGetValue(SparkConnectorConfig.LivyUrl, out var lu) ? lu : null;
        _topic = config[SparkConnectorConfig.OutputTopic];

        _hasSparkUrl = !string.IsNullOrEmpty(sparkUrl);
        _hasLivyUrl = !string.IsNullOrEmpty(livyUrl);

        var timeoutMs = config.TryGetValue(SparkConnectorConfig.TimeoutMs, out var t) && int.TryParse(t, out var tv)
            ? tv : SparkConnectorConfig.DefaultTimeoutMs;

        _pollIntervalMs = config.TryGetValue(SparkConnectorConfig.PollIntervalMs, out var p) && int.TryParse(p, out var pv)
            ? pv : SparkConnectorConfig.DefaultPollIntervalMs;

        _includeAppMetrics = !config.TryGetValue(SparkConnectorConfig.IncludeApplicationMetrics, out var am) ||
            !bool.TryParse(am, out var amv) || amv;

        _includeExecutorMetrics = config.TryGetValue(SparkConnectorConfig.IncludeExecutorMetrics, out var em) &&
            bool.TryParse(em, out var emv) && emv;

        _includeStageMetrics = config.TryGetValue(SparkConnectorConfig.IncludeStageMetrics, out var sm) &&
            bool.TryParse(sm, out var smv) && smv;

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

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        if ((now - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            await Task.Delay(100, cancellationToken);
            return [];
        }

        _lastPoll = now;
        var records = new List<SourceRecord>();

        // Poll Spark Master REST API
        if (_hasSparkUrl)
        {
            await PollSparkAsync(records, now, cancellationToken);
        }

        // Poll Livy REST API
        if (_hasLivyUrl)
        {
            await PollLivyAsync(records, now, cancellationToken);
        }

        return records;
    }

    private async Task PollSparkAsync(List<SourceRecord> records, DateTimeOffset now, CancellationToken ct)
    {
        try
        {
            // Cluster status
            var status = await _client!.GetClusterStatusAsync(ct);
            records.Add(CreateRecord("spark.cluster.status", status));

            // Applications
            if (_includeAppMetrics)
            {
                var apps = await _client.GetApplicationsAsync(ct);
                records.Add(CreateRecord("spark.applications", apps));

                foreach (var app in apps)
                {
                    if (string.IsNullOrEmpty(app.Id)) continue;

                    var currentState = app.Attempts?.FirstOrDefault()?.Completed == true ? "COMPLETED" : "RUNNING";

                    // Check for state changes
                    if (_lastAppStates.TryGetValue(app.Id, out var lastState) && lastState != currentState)
                    {
                        records.Add(CreateRecord("spark.app.state.change", new
                        {
                            appId = app.Id,
                            appName = app.Name,
                            previousState = lastState,
                            currentState,
                            timestamp = now.ToUnixTimeMilliseconds()
                        }));
                    }
                    _lastAppStates[app.Id] = currentState;

                    // Jobs
                    if (currentState == "RUNNING")
                    {
                        try
                        {
                            var jobs = await _client.GetApplicationJobsAsync(app.Id, ct);
                            records.Add(CreateRecord($"spark.app.{app.Id}.jobs", new
                            {
                                appId = app.Id,
                                jobs,
                                timestamp = now.ToUnixTimeMilliseconds()
                            }));

                            // Executors
                            if (_includeExecutorMetrics)
                            {
                                var executors = await _client.GetApplicationExecutorsAsync(app.Id, ct);
                                records.Add(CreateRecord($"spark.app.{app.Id}.executors", new
                                {
                                    appId = app.Id,
                                    executors,
                                    timestamp = now.ToUnixTimeMilliseconds()
                                }));
                            }

                            // Stages
                            if (_includeStageMetrics)
                            {
                                var stages = await _client.GetApplicationStagesAsync(app.Id, ct);
                                records.Add(CreateRecord($"spark.app.{app.Id}.stages", new
                                {
                                    appId = app.Id,
                                    stages,
                                    timestamp = now.ToUnixTimeMilliseconds()
                                }));
                            }
                        }
                        catch
                        {
                            // App may have finished
                        }
                    }
                }
            }
        }
        catch (HttpRequestException ex)
        {
            records.Add(CreateRecord("spark.error", new
            {
                type = "spark_connection_error",
                message = ex.Message,
                timestamp = now.ToUnixTimeMilliseconds()
            }));
        }
    }

    private async Task PollLivyAsync(List<SourceRecord> records, DateTimeOffset now, CancellationToken ct)
    {
        try
        {
            // Sessions
            var sessions = await _client!.GetSessionsAsync(ct);
            records.Add(CreateRecord("livy.sessions", sessions));

            if (sessions.Sessions != null)
            {
                foreach (var session in sessions.Sessions)
                {
                    var currentState = session.State ?? "unknown";

                    if (_lastSessionStates.TryGetValue(session.Id, out var lastState) && lastState != currentState)
                    {
                        records.Add(CreateRecord("livy.session.state.change", new
                        {
                            sessionId = session.Id,
                            sessionName = session.Name,
                            previousState = lastState,
                            currentState,
                            timestamp = now.ToUnixTimeMilliseconds()
                        }));
                    }
                    _lastSessionStates[session.Id] = currentState;
                }
            }

            // Batches
            var batches = await _client.GetBatchesAsync(ct);
            records.Add(CreateRecord("livy.batches", batches));

            if (batches.Sessions != null)
            {
                foreach (var batch in batches.Sessions)
                {
                    var currentState = batch.State ?? "unknown";

                    if (_lastBatchStates.TryGetValue(batch.Id, out var lastState) && lastState != currentState)
                    {
                        records.Add(CreateRecord("livy.batch.state.change", new
                        {
                            batchId = batch.Id,
                            batchName = batch.Name,
                            appId = batch.AppId,
                            previousState = lastState,
                            currentState,
                            timestamp = now.ToUnixTimeMilliseconds()
                        }));
                    }
                    _lastBatchStates[batch.Id] = currentState;
                }
            }
        }
        catch (HttpRequestException ex)
        {
            records.Add(CreateRecord("livy.error", new
            {
                type = "livy_connection_error",
                message = ex.Message,
                timestamp = now.ToUnixTimeMilliseconds()
            }));
        }
    }

    private SourceRecord CreateRecord(string key, object value)
    {
        var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
        var valueBytes = JsonSerializer.SerializeToUtf8Bytes(value);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["type"] = "spark" },
            SourceOffset = new Dictionary<string, object> { ["timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() },
            Topic = _topic,
            Key = keyBytes,
            Value = valueBytes,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["_event_type"] = System.Text.Encoding.UTF8.GetBytes(key)
            }
        };
    }

    public new void Dispose()
    {
        _client?.Dispose();
        base.Dispose();
    }
}
