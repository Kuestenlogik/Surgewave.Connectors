namespace Kuestenlogik.Surgewave.Connector.Spark;

/// <summary>
/// Configuration constants for Spark connectors.
/// </summary>
public static class SparkConnectorConfig
{
    // Connection
    public const string BaseUrl = "spark.base.url";
    public const string LivyUrl = "spark.livy.url";
    public const string Topics = "topics";
    public const string TimeoutMs = "spark.timeout.ms";
    public const int DefaultTimeoutMs = 60000;

    // API Mode
    public const string ApiMode = "spark.api.mode";
    public const string ApiModeSpark = "spark";
    public const string ApiModeLivy = "livy";

    // Authentication
    public const string AuthType = "spark.auth.type";
    public const string AuthTypeNone = "none";
    public const string AuthTypeBasic = "basic";
    public const string AuthTypeKerberos = "kerberos";
    public const string AuthUsername = "spark.auth.username";
    public const string AuthPassword = "spark.auth.password";

    // Livy Session
    public const string SessionKind = "spark.session.kind";
    public const string SessionKindSpark = "spark";
    public const string SessionKindPySpark = "pyspark";
    public const string SessionKindSparkR = "sparkr";
    public const string SessionKindSql = "sql";

    public const string DriverMemory = "spark.driver.memory";
    public const string DefaultDriverMemory = "1g";
    public const string DriverCores = "spark.driver.cores";
    public const int DefaultDriverCores = 1;
    public const string ExecutorMemory = "spark.executor.memory";
    public const string DefaultExecutorMemory = "1g";
    public const string ExecutorCores = "spark.executor.cores";
    public const int DefaultExecutorCores = 1;
    public const string NumExecutors = "spark.num.executors";
    public const int DefaultNumExecutors = 2;

    // Job submission
    public const string MainClass = "spark.main.class";
    public const string JarFile = "spark.jar.file";
    public const string PyFile = "spark.py.file";
    public const string Args = "spark.args";
    public const string Conf = "spark.conf";
    public const string Jars = "spark.jars";
    public const string Files = "spark.files";
    public const string Archives = "spark.archives";
    public const string Queue = "spark.queue";
    public const string Name = "spark.name";

    // Source connector specific
    public const string OutputTopic = "spark.output.topic";
    public const string PollIntervalMs = "spark.poll.interval.ms";
    public const int DefaultPollIntervalMs = 5000;
    public const string IncludeApplicationMetrics = "spark.include.app.metrics";
    public const string IncludeExecutorMetrics = "spark.include.executor.metrics";
    public const string IncludeStageMetrics = "spark.include.stage.metrics";

    // Action types
    public const string ActionType = "spark.action.type";
    public const string ActionTypeSubmit = "submit";
    public const string ActionTypeKill = "kill";
    public const string ActionTypeStatement = "statement";
    public const string ActionTypeCreateSession = "create_session";
    public const string ActionTypeDeleteSession = "delete_session";
}
