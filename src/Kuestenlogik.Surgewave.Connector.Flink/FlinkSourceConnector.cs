using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Flink;

/// <summary>
/// Source connector that polls Flink REST API for cluster status, job metrics, and events.
/// Emits monitoring data to Surgewave topics for observability and alerting.
/// </summary>
[ConnectorMetadata(
    Name = "FlinkSource",
    Description = "Monitor Flink cluster and job metrics",
    Tags = "flink,source,monitoring,metrics")]
public sealed class FlinkSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(FlinkSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(FlinkConnectorConfig.BaseUrl, ConfigType.String, Importance.High,
            "Flink REST API base URL (e.g., http://localhost:8081)")
        .Define(FlinkConnectorConfig.OutputTopic, ConfigType.String, Importance.High,
            "Topic to emit Flink metrics and events", EditorHint.Topic)
        .Define(FlinkConnectorConfig.PollIntervalMs, ConfigType.Int,
            FlinkConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Polling interval in milliseconds")
        .Define(FlinkConnectorConfig.TimeoutMs, ConfigType.Int,
            FlinkConnectorConfig.DefaultTimeoutMs, Importance.Medium,
            "HTTP request timeout in milliseconds")
        .Define(FlinkConnectorConfig.IncludeJobMetrics, ConfigType.Boolean, "true", Importance.Medium,
            "Include job-level metrics")
        .Define(FlinkConnectorConfig.IncludeTaskMetrics, ConfigType.Boolean, "false", Importance.Low,
            "Include task-level metrics")
        .Define(FlinkConnectorConfig.MetricsFilter, ConfigType.String, "", Importance.Low,
            "Comma-separated metric names to fetch (empty = all)")
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

        if (!config.TryGetValue(FlinkConnectorConfig.OutputTopic, out var topic) || string.IsNullOrEmpty(topic))
            throw new ArgumentException($"Missing required config: {FlinkConnectorConfig.OutputTopic}");
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}

#pragma warning disable CA1812
internal sealed class FlinkSourceTask : SourceTask, IDisposable
{
    public override string Version => "1.0.0";

    private FlinkRestClient? _client;
    private string _topic = "";
    private int _pollIntervalMs;
    private bool _includeJobMetrics;
    private bool _includeTaskMetrics;
    private string? _metricsFilter;
    private DateTimeOffset _lastPoll = DateTimeOffset.MinValue;
    private readonly Dictionary<string, string> _lastJobStates = new();

    public override void Start(IDictionary<string, string> config)
    {
        var baseUrl = config[FlinkConnectorConfig.BaseUrl];
        _topic = config[FlinkConnectorConfig.OutputTopic];

        var timeoutMs = config.TryGetValue(FlinkConnectorConfig.TimeoutMs, out var t) && int.TryParse(t, out var tv)
            ? tv : FlinkConnectorConfig.DefaultTimeoutMs;

        _pollIntervalMs = config.TryGetValue(FlinkConnectorConfig.PollIntervalMs, out var p) && int.TryParse(p, out var pv)
            ? pv : FlinkConnectorConfig.DefaultPollIntervalMs;

        _includeJobMetrics = !config.TryGetValue(FlinkConnectorConfig.IncludeJobMetrics, out var jm) ||
            !bool.TryParse(jm, out var jmv) || jmv;

        _includeTaskMetrics = config.TryGetValue(FlinkConnectorConfig.IncludeTaskMetrics, out var tm) &&
            bool.TryParse(tm, out var tmv) && tmv;

        _metricsFilter = config.TryGetValue(FlinkConnectorConfig.MetricsFilter, out var mf) ? mf : null;

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

        try
        {
            // Cluster overview
            var overview = await _client!.GetClusterOverviewAsync(cancellationToken);
            records.Add(CreateRecord("cluster.overview", overview));

            // Jobs overview
            var jobs = await _client.GetJobsAsync(cancellationToken);
            records.Add(CreateRecord("jobs.overview", jobs));

            // Check for job state changes
            foreach (var job in jobs.Jobs)
            {
                if (_lastJobStates.TryGetValue(job.Jid, out var lastState) && lastState != job.State)
                {
                    records.Add(CreateRecord("job.state.change", new
                    {
                        jobId = job.Jid,
                        jobName = job.Name,
                        previousState = lastState,
                        currentState = job.State,
                        timestamp = now.ToUnixTimeMilliseconds()
                    }));
                }
                _lastJobStates[job.Jid] = job.State;

                // Job metrics
                if (_includeJobMetrics && job.State == "RUNNING")
                {
                    try
                    {
                        var metrics = await _client.GetJobMetricsAsync(job.Jid, _metricsFilter, cancellationToken);
                        if (metrics.Count > 0)
                        {
                            records.Add(CreateRecord($"job.{job.Jid}.metrics", new
                            {
                                jobId = job.Jid,
                                jobName = job.Name,
                                metrics,
                                timestamp = now.ToUnixTimeMilliseconds()
                            }));
                        }

                        // Task metrics
                        if (_includeTaskMetrics)
                        {
                            var jobDetails = await _client.GetJobAsync(job.Jid, cancellationToken);
                            foreach (var vertex in jobDetails.Vertices)
                            {
                                if (vertex.Metrics != null)
                                {
                                    records.Add(CreateRecord($"job.{job.Jid}.vertex.{vertex.Id}.metrics", new
                                    {
                                        jobId = job.Jid,
                                        vertexId = vertex.Id,
                                        vertexName = vertex.Name,
                                        metrics = vertex.Metrics,
                                        timestamp = now.ToUnixTimeMilliseconds()
                                    }));
                                }
                            }
                        }
                    }
                    catch
                    {
                        // Job may have finished between list and detail fetch
                    }
                }
            }

            // Task managers
            var taskManagers = await _client.GetTaskManagersAsync(cancellationToken);
            records.Add(CreateRecord("taskmanagers.overview", taskManagers));
        }
        catch (HttpRequestException ex)
        {
            records.Add(CreateRecord("error", new
            {
                type = "connection_error",
                message = ex.Message,
                timestamp = now.ToUnixTimeMilliseconds()
            }));
        }

        return records;
    }

    private SourceRecord CreateRecord(string key, object value)
    {
        var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
        var valueBytes = JsonSerializer.SerializeToUtf8Bytes(value);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["type"] = "flink" },
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
