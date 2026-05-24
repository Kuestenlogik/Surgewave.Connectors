using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Connector.Spark;

/// <summary>
/// HTTP client for Apache Spark REST API and Apache Livy REST API.
/// </summary>
[SuppressMessage("Design", "CA1054:URI parameters should not be strings", Justification = "REST API client uses URL strings for simplicity")]
[SuppressMessage("Design", "CA1056:URI properties should not be strings", Justification = "DTO properties match external API")]
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "DTO properties match external API naming")]
[SuppressMessage("Performance", "CA1869:Cache and reuse JsonSerializerOptions", Justification = "Options are cached in static field")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "StringContent disposed by HttpClient")]
[SuppressMessage("Usage", "CA2234:Pass System.Uri objects instead of strings", Justification = "URL strings are simpler for REST API calls")]
public sealed class SparkRestClient : IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly string _sparkUrl;
    private readonly string? _livyUrl;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public SparkRestClient(string? sparkUrl, string? livyUrl, int timeoutMs = 60000,
        string? authType = null, string? username = null, string? password = null)
    {
        _sparkUrl = sparkUrl?.TrimEnd('/') ?? "";
        _livyUrl = livyUrl?.TrimEnd('/');
        _httpClient = new HttpClient { Timeout = TimeSpan.FromMilliseconds(timeoutMs) };

        if (authType?.Equals("basic", StringComparison.OrdinalIgnoreCase) == true && !string.IsNullOrEmpty(username))
        {
            var credentials = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", credentials);
        }
    }

    #region Spark REST API (Master)

    /// <summary>Get Spark cluster status.</summary>
    public async Task<SparkClusterStatus> GetClusterStatusAsync(CancellationToken ct = default)
    {
        return await GetAsync<SparkClusterStatus>(_sparkUrl, "/json", ct);
    }

    /// <summary>Get all applications.</summary>
    public async Task<List<SparkApplication>> GetApplicationsAsync(CancellationToken ct = default)
    {
        return await GetAsync<List<SparkApplication>>(_sparkUrl, "/api/v1/applications", ct);
    }

    /// <summary>Get application details.</summary>
    public async Task<SparkApplication> GetApplicationAsync(string appId, CancellationToken ct = default)
    {
        return await GetAsync<SparkApplication>(_sparkUrl, $"/api/v1/applications/{appId}", ct);
    }

    /// <summary>Get application jobs.</summary>
    public async Task<List<SparkJob>> GetApplicationJobsAsync(string appId, CancellationToken ct = default)
    {
        return await GetAsync<List<SparkJob>>(_sparkUrl, $"/api/v1/applications/{appId}/jobs", ct);
    }

    /// <summary>Get application stages.</summary>
    public async Task<List<SparkStage>> GetApplicationStagesAsync(string appId, CancellationToken ct = default)
    {
        return await GetAsync<List<SparkStage>>(_sparkUrl, $"/api/v1/applications/{appId}/stages", ct);
    }

    /// <summary>Get application executors.</summary>
    public async Task<List<SparkExecutor>> GetApplicationExecutorsAsync(string appId, CancellationToken ct = default)
    {
        return await GetAsync<List<SparkExecutor>>(_sparkUrl, $"/api/v1/applications/{appId}/executors", ct);
    }

    /// <summary>Get application storage.</summary>
    public async Task<List<SparkRdd>> GetApplicationStorageAsync(string appId, CancellationToken ct = default)
    {
        return await GetAsync<List<SparkRdd>>(_sparkUrl, $"/api/v1/applications/{appId}/storage/rdd", ct);
    }

    /// <summary>Get application environment.</summary>
    public async Task<SparkEnvironment> GetApplicationEnvironmentAsync(string appId, CancellationToken ct = default)
    {
        return await GetAsync<SparkEnvironment>(_sparkUrl, $"/api/v1/applications/{appId}/environment", ct);
    }

    /// <summary>Kill an application (via Spark REST submission API).</summary>
    public async Task<SparkSubmissionResponse> KillApplicationAsync(string submissionId, CancellationToken ct = default)
    {
        return await PostAsync<SparkSubmissionResponse>(_sparkUrl, $"/v1/submissions/kill/{submissionId}", null, ct);
    }

    /// <summary>Get submission status.</summary>
    public async Task<SparkSubmissionStatus> GetSubmissionStatusAsync(string submissionId, CancellationToken ct = default)
    {
        return await GetAsync<SparkSubmissionStatus>(_sparkUrl, $"/v1/submissions/status/{submissionId}", ct);
    }

    /// <summary>Submit a Spark application.</summary>
    public async Task<SparkSubmissionResponse> SubmitApplicationAsync(SparkSubmissionRequest request, CancellationToken ct = default)
    {
        return await PostAsync<SparkSubmissionResponse>(_sparkUrl, "/v1/submissions/create", request, ct);
    }

    #endregion

    #region Livy REST API

    /// <summary>Get all Livy sessions.</summary>
    public async Task<LivySessionsResponse> GetSessionsAsync(CancellationToken ct = default)
    {
        EnsureLivy();
        return await GetAsync<LivySessionsResponse>(_livyUrl!, "/sessions", ct);
    }

    /// <summary>Get Livy session details.</summary>
    public async Task<LivySession> GetSessionAsync(int sessionId, CancellationToken ct = default)
    {
        EnsureLivy();
        return await GetAsync<LivySession>(_livyUrl!, $"/sessions/{sessionId}", ct);
    }

    /// <summary>Create a new Livy session.</summary>
    public async Task<LivySession> CreateSessionAsync(LivySessionRequest request, CancellationToken ct = default)
    {
        EnsureLivy();
        return await PostAsync<LivySession>(_livyUrl!, "/sessions", request, ct);
    }

    /// <summary>Delete a Livy session.</summary>
    public async Task DeleteSessionAsync(int sessionId, CancellationToken ct = default)
    {
        EnsureLivy();
        await DeleteAsync(_livyUrl!, $"/sessions/{sessionId}", ct);
    }

    /// <summary>Get session statements.</summary>
    public async Task<LivyStatementsResponse> GetStatementsAsync(int sessionId, CancellationToken ct = default)
    {
        EnsureLivy();
        return await GetAsync<LivyStatementsResponse>(_livyUrl!, $"/sessions/{sessionId}/statements", ct);
    }

    /// <summary>Execute a statement in a session.</summary>
    public async Task<LivyStatement> ExecuteStatementAsync(int sessionId, string code, string? kind = null, CancellationToken ct = default)
    {
        EnsureLivy();
        var request = new { code, kind };
        return await PostAsync<LivyStatement>(_livyUrl!, $"/sessions/{sessionId}/statements", request, ct);
    }

    /// <summary>Get statement result.</summary>
    public async Task<LivyStatement> GetStatementAsync(int sessionId, int statementId, CancellationToken ct = default)
    {
        EnsureLivy();
        return await GetAsync<LivyStatement>(_livyUrl!, $"/sessions/{sessionId}/statements/{statementId}", ct);
    }

    /// <summary>Cancel a statement.</summary>
    public async Task CancelStatementAsync(int sessionId, int statementId, CancellationToken ct = default)
    {
        EnsureLivy();
        await PostAsync<object>(_livyUrl!, $"/sessions/{sessionId}/statements/{statementId}/cancel", null, ct);
    }

    /// <summary>Get all Livy batches.</summary>
    public async Task<LivyBatchesResponse> GetBatchesAsync(CancellationToken ct = default)
    {
        EnsureLivy();
        return await GetAsync<LivyBatchesResponse>(_livyUrl!, "/batches", ct);
    }

    /// <summary>Submit a Livy batch job.</summary>
    public async Task<LivyBatch> SubmitBatchAsync(LivyBatchRequest request, CancellationToken ct = default)
    {
        EnsureLivy();
        return await PostAsync<LivyBatch>(_livyUrl!, "/batches", request, ct);
    }

    /// <summary>Get Livy batch details.</summary>
    public async Task<LivyBatch> GetBatchAsync(int batchId, CancellationToken ct = default)
    {
        EnsureLivy();
        return await GetAsync<LivyBatch>(_livyUrl!, $"/batches/{batchId}", ct);
    }

    /// <summary>Delete/kill a Livy batch.</summary>
    public async Task DeleteBatchAsync(int batchId, CancellationToken ct = default)
    {
        EnsureLivy();
        await DeleteAsync(_livyUrl!, $"/batches/{batchId}", ct);
    }

    /// <summary>Get Livy batch log.</summary>
    public async Task<LivyLogResponse> GetBatchLogAsync(int batchId, int? from = null, int? size = null, CancellationToken ct = default)
    {
        EnsureLivy();
        var url = $"/batches/{batchId}/log";
        var query = new List<string>();
        if (from.HasValue) query.Add($"from={from}");
        if (size.HasValue) query.Add($"size={size}");
        if (query.Count > 0) url += "?" + string.Join("&", query);
        return await GetAsync<LivyLogResponse>(_livyUrl!, url, ct);
    }

    private void EnsureLivy()
    {
        if (string.IsNullOrEmpty(_livyUrl))
            throw new InvalidOperationException("Livy URL not configured");
    }

    #endregion

    #region HTTP Helpers

    private async Task<T> GetAsync<T>(string baseUrl, string path, CancellationToken ct)
    {
        var response = await _httpClient.GetAsync($"{baseUrl}{path}", ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize<T>(json, JsonOptions)!;
    }

    private async Task<T> PostAsync<T>(string baseUrl, string path, object? body, CancellationToken ct)
    {
        var content = body != null
            ? new StringContent(JsonSerializer.Serialize(body, JsonOptions), Encoding.UTF8, "application/json")
            : null;
        var response = await _httpClient.PostAsync($"{baseUrl}{path}", content, ct);
        response.EnsureSuccessStatusCode();
        var json = await response.Content.ReadAsStringAsync(ct);
        return JsonSerializer.Deserialize<T>(json, JsonOptions)!;
    }

    private async Task DeleteAsync(string baseUrl, string path, CancellationToken ct)
    {
        var response = await _httpClient.DeleteAsync($"{baseUrl}{path}", ct);
        response.EnsureSuccessStatusCode();
    }

    #endregion

    public void Dispose() => _httpClient.Dispose();
}

#region Spark REST API DTOs

[SuppressMessage("Design", "CA1056:URI properties should not be strings", Justification = "DTO property matches Spark REST API response")]
public record SparkClusterStatus
{
    public string? Url { get; init; }
    public List<SparkWorker>? Workers { get; init; }
    public int? Cores { get; init; }
    public int? CoresUsed { get; init; }
    public long? Memory { get; init; }
    public long? MemoryUsed { get; init; }
    public List<SparkActiveApp>? ActiveApps { get; init; }
    public List<SparkCompletedApp>? CompletedApps { get; init; }
    public string? Status { get; init; }
}

public record SparkWorker
{
    public string? Id { get; init; }
    public string? Host { get; init; }
    public int Port { get; init; }
    public int Cores { get; init; }
    public int CoresUsed { get; init; }
    public int CoresFree { get; init; }
    public long Memory { get; init; }
    public long MemoryUsed { get; init; }
    public long MemoryFree { get; init; }
    public string? State { get; init; }
    public long LastHeartbeat { get; init; }
}

public record SparkActiveApp
{
    public string? Id { get; init; }
    public string? Name { get; init; }
    public int Cores { get; init; }
    public long MemoryPerExecutorMb { get; init; }
    public string? User { get; init; }
    public string? State { get; init; }
    public long StartTime { get; init; }
    public long Duration { get; init; }
}

public record SparkCompletedApp
{
    public string? Id { get; init; }
    public string? Name { get; init; }
    public int Cores { get; init; }
    public long MemoryPerExecutorMb { get; init; }
    public string? User { get; init; }
    public string? State { get; init; }
    public long StartTime { get; init; }
    public long EndTime { get; init; }
    public long Duration { get; init; }
}

public record SparkApplication
{
    public string? Id { get; init; }
    public string? Name { get; init; }
    public List<SparkAttempt>? Attempts { get; init; }
}

public record SparkAttempt
{
    public string? AttemptId { get; init; }
    public string? StartTime { get; init; }
    public string? EndTime { get; init; }
    public string? LastUpdated { get; init; }
    public long Duration { get; init; }
    public string? SparkUser { get; init; }
    public bool Completed { get; init; }
    public string? AppSparkVersion { get; init; }
}

public record SparkJob
{
    public int JobId { get; init; }
    public string? Name { get; init; }
    public string? Description { get; init; }
    public string? SubmissionTime { get; init; }
    public string? CompletionTime { get; init; }
    public List<int>? StageIds { get; init; }
    public string? Status { get; init; }
    public int NumTasks { get; init; }
    public int NumActiveTasks { get; init; }
    public int NumCompletedTasks { get; init; }
    public int NumSkippedTasks { get; init; }
    public int NumFailedTasks { get; init; }
    public int NumKilledTasks { get; init; }
    public int NumCompletedIndices { get; init; }
    public int NumActiveStages { get; init; }
    public int NumCompletedStages { get; init; }
    public int NumSkippedStages { get; init; }
    public int NumFailedStages { get; init; }
}

public record SparkStage
{
    public string? Status { get; init; }
    public int StageId { get; init; }
    public int AttemptId { get; init; }
    public int NumTasks { get; init; }
    public int NumActiveTasks { get; init; }
    public int NumCompleteTasks { get; init; }
    public int NumFailedTasks { get; init; }
    public int NumKilledTasks { get; init; }
    public int NumCompletedIndices { get; init; }
    public long ExecutorRunTime { get; init; }
    public long ExecutorCpuTime { get; init; }
    public string? SubmissionTime { get; init; }
    public string? FirstTaskLaunchedTime { get; init; }
    public string? CompletionTime { get; init; }
    public long InputBytes { get; init; }
    public long InputRecords { get; init; }
    public long OutputBytes { get; init; }
    public long OutputRecords { get; init; }
    public long ShuffleReadBytes { get; init; }
    public long ShuffleReadRecords { get; init; }
    public long ShuffleWriteBytes { get; init; }
    public long ShuffleWriteRecords { get; init; }
    public long MemoryBytesSpilled { get; init; }
    public long DiskBytesSpilled { get; init; }
    public string? Name { get; init; }
    public string? Details { get; init; }
    public string? SchedulingPool { get; init; }
}

public record SparkExecutor
{
    public string? Id { get; init; }
    public string? HostPort { get; init; }
    public bool IsActive { get; init; }
    public int RddBlocks { get; init; }
    public long MemoryUsed { get; init; }
    public long DiskUsed { get; init; }
    public int TotalCores { get; init; }
    public int MaxTasks { get; init; }
    public int ActiveTasks { get; init; }
    public int FailedTasks { get; init; }
    public int CompletedTasks { get; init; }
    public int TotalTasks { get; init; }
    public long TotalDuration { get; init; }
    public long TotalGcTime { get; init; }
    public long TotalInputBytes { get; init; }
    public long TotalShuffleRead { get; init; }
    public long TotalShuffleWrite { get; init; }
    public bool IsBlacklisted { get; init; }
    public long MaxMemory { get; init; }
    public string? AddTime { get; init; }
    public string? RemoveTime { get; init; }
    public string? RemoveReason { get; init; }
}

public record SparkRdd
{
    public int Id { get; init; }
    public string? Name { get; init; }
    public int NumPartitions { get; init; }
    public int NumCachedPartitions { get; init; }
    public string? StorageLevel { get; init; }
    public long MemoryUsed { get; init; }
    public long DiskUsed { get; init; }
}

public record SparkEnvironment
{
    public SparkRuntime? Runtime { get; init; }
    public List<List<string>>? SparkProperties { get; init; }
    public List<List<string>>? SystemProperties { get; init; }
    public List<List<string>>? ClasspathEntries { get; init; }
}

public record SparkRuntime
{
    public string? JavaVersion { get; init; }
    public string? JavaHome { get; init; }
    public string? ScalaVersion { get; init; }
}

public record SparkSubmissionRequest
{
    public string? Action { get; init; } = "CreateSubmissionRequest";
    public string? AppResource { get; init; }
    public List<string>? AppArgs { get; init; }
    public string? ClientSparkVersion { get; init; }
    public string? MainClass { get; init; }
    public Dictionary<string, string>? EnvironmentVariables { get; init; }
    public Dictionary<string, string>? SparkProperties { get; init; }
}

public record SparkSubmissionResponse
{
    public string? Action { get; init; }
    public string? Message { get; init; }
    public string? ServerSparkVersion { get; init; }
    public string? SubmissionId { get; init; }
    public bool Success { get; init; }
}

public record SparkSubmissionStatus
{
    public string? Action { get; init; }
    public string? DriverState { get; init; }
    public string? ServerSparkVersion { get; init; }
    public string? SubmissionId { get; init; }
    public bool Success { get; init; }
    public string? WorkerHostPort { get; init; }
    public string? WorkerId { get; init; }
}

#endregion

#region Livy REST API DTOs

public record LivySessionsResponse
{
    public int From { get; init; }
    public int Total { get; init; }
    public List<LivySession>? Sessions { get; init; }
}

public record LivySession
{
    public int Id { get; init; }
    public string? Name { get; init; }
    public string? AppId { get; init; }
    public string? Owner { get; init; }
    public string? ProxyUser { get; init; }
    public string? State { get; init; }
    public string? Kind { get; init; }
    public Dictionary<string, string>? AppInfo { get; init; }
    public List<string>? Log { get; init; }
}

public record LivySessionRequest
{
    public string? Kind { get; init; }
    public string? ProxyUser { get; init; }
    public List<string>? Jars { get; init; }
    public List<string>? PyFiles { get; init; }
    public List<string>? Files { get; init; }
    public string? DriverMemory { get; init; }
    public int? DriverCores { get; init; }
    public string? ExecutorMemory { get; init; }
    public int? ExecutorCores { get; init; }
    public int? NumExecutors { get; init; }
    public List<string>? Archives { get; init; }
    public string? Queue { get; init; }
    public string? Name { get; init; }
    public Dictionary<string, string>? Conf { get; init; }
    public int? HeartbeatTimeoutInSecond { get; init; }
}

public record LivyStatementsResponse
{
    public int TotalStatements { get; init; }
    public List<LivyStatement>? Statements { get; init; }
}

public record LivyStatement
{
    public int Id { get; init; }
    public string? Code { get; init; }
    public string? State { get; init; }
    public LivyStatementOutput? Output { get; init; }
    public double Progress { get; init; }
    public long Started { get; init; }
    public long Completed { get; init; }
}

public record LivyStatementOutput
{
    public string? Status { get; init; }
    public int ExecutionCount { get; init; }
    public LivyOutputData? Data { get; init; }
    public string? Ename { get; init; }
    public string? Evalue { get; init; }
    public List<string>? Traceback { get; init; }
}

public record LivyOutputData
{
    [JsonPropertyName("text/plain")]
    public string? TextPlain { get; init; }
    [JsonPropertyName("application/json")]
    public JsonElement? ApplicationJson { get; init; }
}

public record LivyBatchesResponse
{
    public int From { get; init; }
    public int Total { get; init; }
    public List<LivyBatch>? Sessions { get; init; }
}

public record LivyBatch
{
    public int Id { get; init; }
    public string? Name { get; init; }
    public string? AppId { get; init; }
    public string? Owner { get; init; }
    public string? ProxyUser { get; init; }
    public string? State { get; init; }
    public Dictionary<string, string>? AppInfo { get; init; }
    public List<string>? Log { get; init; }
}

public record LivyBatchRequest
{
    public string? File { get; init; }
    public string? ProxyUser { get; init; }
    public string? ClassName { get; init; }
    public List<string>? Args { get; init; }
    public List<string>? Jars { get; init; }
    public List<string>? PyFiles { get; init; }
    public List<string>? Files { get; init; }
    public string? DriverMemory { get; init; }
    public int? DriverCores { get; init; }
    public string? ExecutorMemory { get; init; }
    public int? ExecutorCores { get; init; }
    public int? NumExecutors { get; init; }
    public List<string>? Archives { get; init; }
    public string? Queue { get; init; }
    public string? Name { get; init; }
    public Dictionary<string, string>? Conf { get; init; }
}

public record LivyLogResponse
{
    public int Id { get; init; }
    public int From { get; init; }
    public int Total { get; init; }
    public List<string>? Log { get; init; }
}

#endregion
