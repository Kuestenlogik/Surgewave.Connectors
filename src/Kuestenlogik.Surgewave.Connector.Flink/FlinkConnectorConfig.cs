namespace Kuestenlogik.Surgewave.Connector.Flink;

/// <summary>
/// Configuration constants for Flink connectors.
/// </summary>
public static class FlinkConnectorConfig
{
    // Connection
    public const string BaseUrl = "flink.base.url";
    public const string Topics = "topics";
    public const string TimeoutMs = "flink.timeout.ms";
    public const int DefaultTimeoutMs = 30000;

    // Authentication
    public const string AuthType = "flink.auth.type";
    public const string AuthTypeNone = "none";
    public const string AuthTypeBasic = "basic";
    public const string AuthTypeBearer = "bearer";
    public const string AuthUsername = "flink.auth.username";
    public const string AuthPassword = "flink.auth.password";
    public const string AuthToken = "flink.auth.token";

    // Job Management
    public const string JobId = "flink.job.id";
    public const string JarId = "flink.jar.id";
    public const string Parallelism = "flink.parallelism";
    public const int DefaultParallelism = 1;
    public const string SavepointPath = "flink.savepoint.path";
    public const string ProgramArgs = "flink.program.args";
    public const string EntryClass = "flink.entry.class";
    public const string AllowNonRestoredState = "flink.allow.non.restored.state";

    // Source connector specific
    public const string PollIntervalMs = "flink.poll.interval.ms";
    public const int DefaultPollIntervalMs = 5000;
    public const string MetricsFilter = "flink.metrics.filter";
    public const string IncludeJobMetrics = "flink.include.job.metrics";
    public const string IncludeTaskMetrics = "flink.include.task.metrics";
    public const string IncludeOperatorMetrics = "flink.include.operator.metrics";

    // Sink connector specific
    public const string OutputTopic = "flink.output.topic";
    public const string ActionType = "flink.action.type";
    public const string ActionTypeSubmit = "submit";
    public const string ActionTypeCancel = "cancel";
    public const string ActionTypeSavepoint = "savepoint";
    public const string ActionTypeRescale = "rescale";
}
