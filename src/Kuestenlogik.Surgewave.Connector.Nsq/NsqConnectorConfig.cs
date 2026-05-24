namespace Kuestenlogik.Surgewave.Connector.Nsq;

/// <summary>
/// Configuration constants for NSQ connectors.
/// </summary>
public static class NsqConnectorConfig
{
    // Connection settings
    public const string NsqdAddress = "nsq.nsqd.address";
    public const string NsqLookupdAddresses = "nsq.nsqlookupd.addresses";

    // Topic/Channel settings
    public const string NsqTopic = "nsq.topic";
    public const string NsqChannel = "nsq.channel";
    public const string Topic = "topic";
    public const string Topics = "topics";

    // Consumer settings
    public const string MaxInFlight = "nsq.max.in.flight";
    public const string MaxAttempts = "nsq.max.attempts";
    public const string RequeueDelayMs = "nsq.requeue.delay.ms";
    public const string BatchSize = "nsq.batch.size";
    public const string PollTimeoutMs = "nsq.poll.timeout.ms";

    // Producer settings
    public const string PublishTimeoutMs = "nsq.publish.timeout.ms";
    public const string Retries = "nsq.retries";

    // TLS settings
    public const string TlsEnabled = "nsq.tls.enabled";
    public const string TlsInsecureSkipVerify = "nsq.tls.insecure.skip.verify";

    // Authentication
    public const string AuthSecret = "nsq.auth.secret";

    // Defaults
    public const string DefaultNsqdAddress = "127.0.0.1:4150";
    public const string DefaultNsqLookupdAddress = "127.0.0.1:4161";
    public const string DefaultChannel = "surgewave-connector";
    public const int DefaultMaxInFlight = 200;
    public const int DefaultMaxAttempts = 5;
    public const int DefaultRequeueDelayMs = 5000;
    public const int DefaultBatchSize = 100;
    public const int DefaultPollTimeoutMs = 1000;
    public const int DefaultPublishTimeoutMs = 30000;
    public const int DefaultRetries = 3;

    // Offset tracking keys
    public const string OffsetTimestamp = "timestamp";
    public const string OffsetAttempts = "attempts";
    public const string OffsetMessageId = "message_id";
}
