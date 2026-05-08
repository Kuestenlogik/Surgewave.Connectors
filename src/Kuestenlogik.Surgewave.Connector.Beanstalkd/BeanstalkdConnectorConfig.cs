namespace Kuestenlogik.Surgewave.Connector.Beanstalkd;

/// <summary>
/// Configuration constants for Beanstalkd connector.
/// </summary>
public static class BeanstalkdConnectorConfig
{
    // Connection settings
    public const string Host = "beanstalkd.host";
    public const string Port = "beanstalkd.port";

    // Tube settings
    public const string Tube = "beanstalkd.tube";

    // Source settings (reserve)
    public const string Topic = "topic";
    public const string ReserveTimeoutSeconds = "beanstalkd.reserve.timeout.seconds";
    public const string BatchSize = "beanstalkd.batch.size";
    public const string PollTimeoutMs = "beanstalkd.poll.timeout.ms";

    // Sink settings (put)
    public const string Topics = "topics";
    public const string Priority = "beanstalkd.priority";
    public const string DelaySeconds = "beanstalkd.delay.seconds";
    public const string TtrSeconds = "beanstalkd.ttr.seconds";

    // Offset tracking keys
    public const string OffsetJobId = "job.id";
    public const string OffsetTimestamp = "timestamp";

    // Defaults
    public const string DefaultHost = "localhost";
    public const int DefaultPort = 11300;
    public const string DefaultTube = "default";
    public const int DefaultReserveTimeoutSeconds = 5;
    public const int DefaultBatchSize = 100;
    public const int DefaultPollTimeoutMs = 1000;
    public const uint DefaultPriority = 1024;
    public const int DefaultDelaySeconds = 0;
    public const int DefaultTtrSeconds = 60;
}
