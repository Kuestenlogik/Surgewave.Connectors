using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Connector.Flink;

/// <summary>
/// HTTP client for Apache Flink REST API.
/// Supports Flink 1.13+ REST API endpoints.
/// </summary>
[SuppressMessage("Design", "CA1054:URI parameters should not be strings", Justification = "REST API client uses URL strings for simplicity")]
[SuppressMessage("Design", "CA1056:URI properties should not be strings", Justification = "DTO properties match external API")]
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "DTO properties match external API naming")]
[SuppressMessage("Performance", "CA1869:Cache and reuse JsonSerializerOptions", Justification = "Options are cached in static field")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "StringContent disposed by HttpClient")]
[SuppressMessage("Usage", "CA2234:Pass System.Uri objects instead of strings", Justification = "URL strings are simpler for REST API calls")]
public sealed class FlinkRestClient : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public FlinkRestClient(string baseUrl, int timeoutMs = 30000, string? authType = null,
        string? username = null, string? password = null, string? token = null)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _httpClient = new HttpClient { Timeout = TimeSpan.FromMilliseconds(timeoutMs) };

        // Configure authentication
        switch (authType?.ToLowerInvariant())
        {
            case "basic" when !string.IsNullOrEmpty(username):
                var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
                _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
                break;
            case "bearer" when !string.IsNullOrEmpty(token):
                _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
                break;
        }
    }

    #region Cluster Information

    /// <summary>Get cluster overview.</summary>
    public async Task<ClusterOverview> GetClusterOverviewAsync(CancellationToken ct = default)
    {
        return await GetAsync<ClusterOverview>("/overview", ct);
    }

    /// <summary>Get cluster configuration.</summary>
    public async Task<List<ConfigEntry>> GetClusterConfigAsync(CancellationToken ct = default)
    {
        return await GetAsync<List<ConfigEntry>>("/jobmanager/config", ct);
    }

    /// <summary>Get task managers.</summary>
    public async Task<TaskManagersInfo> GetTaskManagersAsync(CancellationToken ct = default)
    {
        return await GetAsync<TaskManagersInfo>("/taskmanagers", ct);
    }

    #endregion

    #region Job Management

    /// <summary>Get all jobs.</summary>
    public async Task<JobsOverview> GetJobsAsync(CancellationToken ct = default)
    {
        return await GetAsync<JobsOverview>("/jobs/overview", ct);
    }

    /// <summary>Get job details.</summary>
    public async Task<JobDetails> GetJobAsync(string jobId, CancellationToken ct = default)
    {
        return await GetAsync<JobDetails>($"/jobs/{jobId}", ct);
    }

    /// <summary>Get job exceptions.</summary>
    public async Task<JobExceptions> GetJobExceptionsAsync(string jobId, CancellationToken ct = default)
    {
        return await GetAsync<JobExceptions>($"/jobs/{jobId}/exceptions", ct);
    }

    /// <summary>Get job metrics.</summary>
    public async Task<List<MetricValue>> GetJobMetricsAsync(string jobId, string? filter = null, CancellationToken ct = default)
    {
        var url = $"/jobs/{jobId}/metrics";
        if (!string.IsNullOrEmpty(filter))
            url += $"?get={filter}";
        return await GetAsync<List<MetricValue>>(url, ct);
    }

    /// <summary>Cancel a job.</summary>
    public async Task CancelJobAsync(string jobId, CancellationToken ct = default)
    {
        await PatchAsync($"/jobs/{jobId}?mode=cancel", ct);
    }

    /// <summary>Stop a job with savepoint.</summary>
    public async Task<TriggerResponse> StopJobWithSavepointAsync(string jobId, string? targetDirectory = null, bool drain = false, CancellationToken ct = default)
    {
        var request = new StopWithSavepointRequest
        {
            TargetDirectory = targetDirectory,
            Drain = drain
        };
        return await PostAsync<TriggerResponse>($"/jobs/{jobId}/stop", request, ct);
    }

    /// <summary>Trigger a savepoint.</summary>
    public async Task<TriggerResponse> TriggerSavepointAsync(string jobId, string? targetDirectory = null, bool cancelJob = false, CancellationToken ct = default)
    {
        var request = new SavepointTriggerRequest
        {
            TargetDirectory = targetDirectory,
            CancelJob = cancelJob
        };
        return await PostAsync<TriggerResponse>($"/jobs/{jobId}/savepoints", request, ct);
    }

    /// <summary>Get savepoint status.</summary>
    public async Task<SavepointInfo> GetSavepointStatusAsync(string jobId, string triggerId, CancellationToken ct = default)
    {
        return await GetAsync<SavepointInfo>($"/jobs/{jobId}/savepoints/{triggerId}", ct);
    }

    /// <summary>Rescale a job.</summary>
    public async Task<TriggerResponse> RescaleJobAsync(string jobId, int parallelism, CancellationToken ct = default)
    {
        return await PatchAsync<TriggerResponse>($"/jobs/{jobId}/rescaling?parallelism={parallelism}", ct);
    }

    #endregion

    #region JAR Management

    /// <summary>Get uploaded JARs.</summary>
    public async Task<JarsInfo> GetJarsAsync(CancellationToken ct = default)
    {
        return await GetAsync<JarsInfo>("/jars", ct);
    }

    /// <summary>Upload a JAR file.</summary>
    public async Task<JarUploadResponse> UploadJarAsync(string filePath, CancellationToken ct = default)
    {
        using var content = new MultipartFormDataContent();
        using var fileStream = File.OpenRead(filePath);
        using var streamContent = new StreamContent(fileStream);
        content.Add(streamContent, "jarfile", Path.GetFileName(filePath));

        var response = await _httpClient.PostAsync($"{_baseUrl}/jars/upload", content, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize<JarUploadResponse>(json, JsonOptions)!;
    }

    /// <summary>Delete a JAR.</summary>
    public async Task DeleteJarAsync(string jarId, CancellationToken ct = default)
    {
        await DeleteAsync($"/jars/{jarId}", ct);
    }

    /// <summary>Run a JAR (submit job).</summary>
    public async Task<JobSubmitResponse> RunJarAsync(string jarId, JarRunRequest request, CancellationToken ct = default)
    {
        return await PostAsync<JobSubmitResponse>($"/jars/{jarId}/run", request, ct);
    }

    /// <summary>Get JAR plan.</summary>
    public async Task<JarPlanResponse> GetJarPlanAsync(string jarId, string? entryClass = null, int? parallelism = null, CancellationToken ct = default)
    {
        var url = $"/jars/{jarId}/plan";
        var query = new List<string>();
        if (!string.IsNullOrEmpty(entryClass)) query.Add($"entry-class={entryClass}");
        if (parallelism.HasValue) query.Add($"parallelism={parallelism}");
        if (query.Count > 0) url += "?" + string.Join("&", query);
        return await GetAsync<JarPlanResponse>(url, ct);
    }

    #endregion

    #region Checkpoints

    /// <summary>Get checkpoint statistics.</summary>
    public async Task<CheckpointStatistics> GetCheckpointStatsAsync(string jobId, CancellationToken ct = default)
    {
        return await GetAsync<CheckpointStatistics>($"/jobs/{jobId}/checkpoints", ct);
    }

    /// <summary>Get checkpoint configuration.</summary>
    public async Task<CheckpointConfig> GetCheckpointConfigAsync(string jobId, CancellationToken ct = default)
    {
        return await GetAsync<CheckpointConfig>($"/jobs/{jobId}/checkpoints/config", ct);
    }

    #endregion

    #region HTTP Helpers

    private async Task<T> GetAsync<T>(string path, CancellationToken ct)
    {
        var response = await _httpClient.GetAsync($"{_baseUrl}{path}", ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize<T>(json, JsonOptions)!;
    }

    private async Task<T> PostAsync<T>(string path, object? body, CancellationToken ct)
    {
        var content = body != null
            ? new StringContent(JsonSerializer.Serialize(body, JsonOptions), Encoding.UTF8, "application/json")
            : null;
        var response = await _httpClient.PostAsync($"{_baseUrl}{path}", content, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize<T>(json, JsonOptions)!;
    }

    private async Task PatchAsync(string path, CancellationToken ct)
    {
        var request = new HttpRequestMessage(HttpMethod.Patch, $"{_baseUrl}{path}");
        var response = await _httpClient.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();
    }

    private async Task<T> PatchAsync<T>(string path, CancellationToken ct)
    {
        var request = new HttpRequestMessage(HttpMethod.Patch, $"{_baseUrl}{path}");
        var response = await _httpClient.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize<T>(json, JsonOptions)!;
    }

    private async Task DeleteAsync(string path, CancellationToken ct)
    {
        var response = await _httpClient.DeleteAsync($"{_baseUrl}{path}", ct);
        response.EnsureSuccessStatusCode();
    }

    #endregion

    public void Dispose() => _httpClient.Dispose();
}

#region DTOs

public record ClusterOverview
{
    public int TaskManagers { get; init; }
    public int SlotsTotal { get; init; }
    public int SlotsAvailable { get; init; }
    public int JobsRunning { get; init; }
    public int JobsFinished { get; init; }
    public int JobsCancelled { get; init; }
    public int JobsFailed { get; init; }
    public string? FlinkVersion { get; init; }
    public string? FlinkCommit { get; init; }
}

public record ConfigEntry
{
    public string Key { get; init; } = "";
    public string Value { get; init; } = "";
}

public record TaskManagersInfo
{
    public List<TaskManagerInfo> TaskManagers { get; init; } = [];
}

public record TaskManagerInfo
{
    public string Id { get; init; } = "";
    public string Path { get; init; } = "";
    public int DataPort { get; init; }
    public int JmxPort { get; init; }
    public long TimeSinceLastHeartbeat { get; init; }
    public int SlotsNumber { get; init; }
    public int FreeSlots { get; init; }
    public TaskManagerResources? TotalResource { get; init; }
    public TaskManagerResources? FreeResource { get; init; }
    public string? Hardware { get; init; }
    public TaskManagerMemory? MemoryConfiguration { get; init; }
}

public record TaskManagerResources
{
    public double CpuCores { get; init; }
    public long TaskHeapMemory { get; init; }
    public long TaskOffHeapMemory { get; init; }
    public long ManagedMemory { get; init; }
    public long NetworkMemory { get; init; }
}

public record TaskManagerMemory
{
    public long FrameworkHeap { get; init; }
    public long TaskHeap { get; init; }
    public long FrameworkOffHeap { get; init; }
    public long TaskOffHeap { get; init; }
    public long NetworkMemory { get; init; }
    public long ManagedMemory { get; init; }
    public long JvmMetaspace { get; init; }
    public long JvmOverhead { get; init; }
    public long TotalFlinkMemory { get; init; }
    public long TotalProcessMemory { get; init; }
}

public record JobsOverview
{
    public List<JobOverview> Jobs { get; init; } = [];
}

public record JobOverview
{
    public string Jid { get; init; } = "";
    public string Name { get; init; } = "";
    public string State { get; init; } = "";
    [JsonPropertyName("start-time")]
    public long StartTime { get; init; }
    [JsonPropertyName("end-time")]
    public long EndTime { get; init; }
    public long Duration { get; init; }
    [JsonPropertyName("last-modification")]
    public long LastModification { get; init; }
    public JobTaskCounts? Tasks { get; init; }
}

public record JobTaskCounts
{
    public int Total { get; init; }
    public int Created { get; init; }
    public int Scheduled { get; init; }
    public int Deploying { get; init; }
    public int Running { get; init; }
    public int Finished { get; init; }
    public int Canceling { get; init; }
    public int Canceled { get; init; }
    public int Failed { get; init; }
    public int Reconciling { get; init; }
}

public record JobDetails
{
    public string Jid { get; init; } = "";
    public string Name { get; init; } = "";
    public string State { get; init; } = "";
    [JsonPropertyName("start-time")]
    public long StartTime { get; init; }
    [JsonPropertyName("end-time")]
    public long EndTime { get; init; }
    public long Duration { get; init; }
    public long Now { get; init; }
    public List<JobVertex> Vertices { get; init; } = [];
    public Dictionary<string, long>? Timestamps { get; init; }
    [JsonPropertyName("status-counts")]
    public JobTaskCounts? StatusCounts { get; init; }
    public JobPlan? Plan { get; init; }
}

public record JobVertex
{
    public string Id { get; init; } = "";
    public string Name { get; init; } = "";
    public int Parallelism { get; init; }
    public string Status { get; init; } = "";
    [JsonPropertyName("start-time")]
    public long StartTime { get; init; }
    [JsonPropertyName("end-time")]
    public long EndTime { get; init; }
    public long Duration { get; init; }
    public JobTaskCounts? Tasks { get; init; }
    public VertexMetrics? Metrics { get; init; }
}

public record VertexMetrics
{
    [JsonPropertyName("read-bytes")]
    public long ReadBytes { get; init; }
    [JsonPropertyName("read-bytes-complete")]
    public bool ReadBytesComplete { get; init; }
    [JsonPropertyName("write-bytes")]
    public long WriteBytes { get; init; }
    [JsonPropertyName("write-bytes-complete")]
    public bool WriteBytesComplete { get; init; }
    [JsonPropertyName("read-records")]
    public long ReadRecords { get; init; }
    [JsonPropertyName("read-records-complete")]
    public bool ReadRecordsComplete { get; init; }
    [JsonPropertyName("write-records")]
    public long WriteRecords { get; init; }
    [JsonPropertyName("write-records-complete")]
    public bool WriteRecordsComplete { get; init; }
}

public record JobPlan
{
    public string Jid { get; init; } = "";
    public string Name { get; init; } = "";
    public List<PlanNode> Nodes { get; init; } = [];
}

public record PlanNode
{
    public string Id { get; init; } = "";
    public string Description { get; init; } = "";
    public int Parallelism { get; init; }
    public List<PlanNodeInput>? Inputs { get; init; }
}

public record PlanNodeInput
{
    public string Id { get; init; } = "";
    [JsonPropertyName("ship_strategy")]
    public string? ShipStrategy { get; init; }
    public string? Exchange { get; init; }
}

public record JobExceptions
{
    [JsonPropertyName("root-exception")]
    public string? RootException { get; init; }
    public long? Timestamp { get; init; }
    [JsonPropertyName("all-exceptions")]
    public List<ExceptionInfo>? AllExceptions { get; init; }
    public bool Truncated { get; init; }
}

public record ExceptionInfo
{
    public string Exception { get; init; } = "";
    public string Task { get; init; } = "";
    public string Location { get; init; } = "";
    public long Timestamp { get; init; }
    public string? TaskManagerId { get; init; }
}

public record MetricValue
{
    public string Id { get; init; } = "";
    public string? Value { get; init; }
}

public record TriggerResponse
{
    [JsonPropertyName("request-id")]
    public string RequestId { get; init; } = "";
}

public record StopWithSavepointRequest
{
    public string? TargetDirectory { get; init; }
    public bool Drain { get; init; }
}

public record SavepointTriggerRequest
{
    public string? TargetDirectory { get; init; }
    public bool CancelJob { get; init; }
}

public record SavepointInfo
{
    public SavepointStatus Status { get; init; } = new();
    public SavepointOperation? Operation { get; init; }
}

public record SavepointStatus
{
    public string Id { get; init; } = "";
}

public record SavepointOperation
{
    public string? Location { get; init; }
    [JsonPropertyName("failure-cause")]
    public FailureCause? FailureCause { get; init; }
}

public record FailureCause
{
    public string? Class { get; init; }
    [JsonPropertyName("stack-trace")]
    public string? StackTrace { get; init; }
}

public record JarsInfo
{
    public string Address { get; init; } = "";
    public List<JarInfo> Files { get; init; } = [];
}

public record JarInfo
{
    public string Id { get; init; } = "";
    public string Name { get; init; } = "";
    public long Uploaded { get; init; }
    public List<JarEntryInfo>? Entry { get; init; }
}

public record JarEntryInfo
{
    public string Name { get; init; } = "";
    public string? Description { get; init; }
}

public record JarUploadResponse
{
    public string Filename { get; init; } = "";
    public string Status { get; init; } = "";
}

public record JarRunRequest
{
    public bool? AllowNonRestoredState { get; init; }
    public string? EntryClass { get; init; }
    public int? Parallelism { get; init; }
    public string? ProgramArgs { get; init; }
    public string? SavepointPath { get; init; }
    public RestoreMode? RestoreMode { get; init; }
}

[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Enum values match Flink REST API naming")]
public enum RestoreMode
{
    CLAIM,
    NO_CLAIM,
    LEGACY
}

public record JobSubmitResponse
{
    public string JobId { get; init; } = "";
}

public record JarPlanResponse
{
    public JobPlan Plan { get; init; } = new();
}

public record CheckpointStatistics
{
    public CheckpointCounts Counts { get; init; } = new();
    public CheckpointSummary? Summary { get; init; }
    public CheckpointLatest? Latest { get; init; }
    public List<CheckpointHistoryEntry>? History { get; init; }
}

public record CheckpointCounts
{
    public int Restored { get; init; }
    public int Total { get; init; }
    public int InProgress { get; init; }
    public int Completed { get; init; }
    public int Failed { get; init; }
}

public record CheckpointSummary
{
    public CheckpointMinMaxAvg? StateSizeBytes { get; init; }
    [JsonPropertyName("end_to_end_duration")]
    public CheckpointMinMaxAvg? EndToEndDuration { get; init; }
    public CheckpointMinMaxAvg? ProcessedData { get; init; }
    public CheckpointMinMaxAvg? PersistedData { get; init; }
}

public record CheckpointMinMaxAvg
{
    public long Min { get; init; }
    public long Max { get; init; }
    public long Avg { get; init; }
}

public record CheckpointLatest
{
    public CheckpointInfo? Completed { get; init; }
    public CheckpointInfo? Savepoint { get; init; }
    public CheckpointFailure? Failed { get; init; }
    public CheckpointInfo? Restored { get; init; }
}

public record CheckpointInfo
{
    public long Id { get; init; }
    public string Status { get; init; } = "";
    [JsonPropertyName("is_savepoint")]
    public bool IsSavepoint { get; init; }
    [JsonPropertyName("trigger_timestamp")]
    public long TriggerTimestamp { get; init; }
    [JsonPropertyName("latest_ack_timestamp")]
    public long LatestAckTimestamp { get; init; }
    [JsonPropertyName("state_size")]
    public long StateSize { get; init; }
    [JsonPropertyName("end_to_end_duration")]
    public long EndToEndDuration { get; init; }
    [JsonPropertyName("processed_data")]
    public long ProcessedData { get; init; }
    [JsonPropertyName("persisted_data")]
    public long PersistedData { get; init; }
    [JsonPropertyName("num_subtasks")]
    public int NumSubtasks { get; init; }
    [JsonPropertyName("num_acknowledged_subtasks")]
    public int NumAcknowledgedSubtasks { get; init; }
    public string? ExternalPath { get; init; }
}

public record CheckpointFailure
{
    public long Id { get; init; }
    public string Status { get; init; } = "";
    [JsonPropertyName("trigger_timestamp")]
    public long TriggerTimestamp { get; init; }
    [JsonPropertyName("latest_ack_timestamp")]
    public long LatestAckTimestamp { get; init; }
    [JsonPropertyName("failure_timestamp")]
    public long FailureTimestamp { get; init; }
    [JsonPropertyName("failure_message")]
    public string? FailureMessage { get; init; }
}

public record CheckpointHistoryEntry
{
    public long Id { get; init; }
    public string Status { get; init; } = "";
    [JsonPropertyName("is_savepoint")]
    public bool IsSavepoint { get; init; }
    [JsonPropertyName("trigger_timestamp")]
    public long TriggerTimestamp { get; init; }
    [JsonPropertyName("latest_ack_timestamp")]
    public long LatestAckTimestamp { get; init; }
    [JsonPropertyName("state_size")]
    public long StateSize { get; init; }
    [JsonPropertyName("end_to_end_duration")]
    public long EndToEndDuration { get; init; }
}

public record CheckpointConfig
{
    public string Mode { get; init; } = "";
    public long Interval { get; init; }
    public long Timeout { get; init; }
    [JsonPropertyName("min_pause")]
    public long MinPause { get; init; }
    [JsonPropertyName("max_concurrent")]
    public int MaxConcurrent { get; init; }
    [JsonPropertyName("externalization")]
    public ExternalizationConfig? Externalization { get; init; }
    [JsonPropertyName("unaligned_checkpoints")]
    public bool UnalignedCheckpoints { get; init; }
    [JsonPropertyName("tolerable_failed_checkpoints")]
    public int? TolerableFailedCheckpoints { get; init; }
}

public record ExternalizationConfig
{
    public bool Enabled { get; init; }
    [JsonPropertyName("delete_on_cancellation")]
    public bool DeleteOnCancellation { get; init; }
}

#endregion
