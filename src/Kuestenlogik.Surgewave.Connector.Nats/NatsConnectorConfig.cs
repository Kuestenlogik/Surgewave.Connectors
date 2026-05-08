namespace Kuestenlogik.Surgewave.Connector.Nats;

/// <summary>
/// Configuration constants for NATS connectors (Core NATS and JetStream).
/// </summary>
public static class NatsConnectorConfig
{
    // Connection
    public const string Url = "nats.url";
    public const string DefaultUrl = "nats://localhost:4222";
    public const string CredentialsFile = "nats.credentials.file";
    public const string Token = "nats.token";
    public const string Username = "nats.username";
    public const string Password = "nats.password";

    // TLS
    public const string UseTls = "tls.enabled";
    public const string TlsEnabled = "tls.enabled"; // Alias
    public const bool DefaultUseTls = false;
    public const string TlsCertificatePath = "tls.certificate.path";
    public const string TlsKeyPath = "tls.key.path";
    public const string TlsCaPath = "tls.ca.path";
    public const string TlsValidateCertificate = "tls.validate.certificate";
    public const bool DefaultTlsValidateCertificate = true;

    // Topics
    public const string Topic = "topic";
    public const string Topics = "topics";

    // Core NATS (non-JetStream)
    public const string Subject = "subject";
    public const string SubjectTemplate = "subject.template";
    public const string QueueGroup = "queue.group";
    public const string BatchSize = "batch.size";
    public const int DefaultBatchSize = 100;
    public const string PollTimeoutMs = "poll.timeout.ms";
    public const int DefaultPollTimeoutMs = 1000;

    // JetStream Consumer
    public const string StreamName = "stream.name";
    public const string ConsumerName = "consumer.name";
    public const string ConsumerDurable = "consumer.durable";
    public const bool DefaultConsumerDurable = true;
    public const string DeliverPolicy = "deliver.policy";
    public const string DefaultDeliverPolicy = "all";
    public const string AckPolicy = "ack.policy";
    public const string DefaultAckPolicy = "explicit";
    public const string MaxAckPending = "max.ack.pending";
    public const int DefaultMaxAckPending = 1000;
    public const string FetchBatchSize = "fetch.batch.size";
    public const int DefaultFetchBatchSize = 100;
    public const string FetchTimeoutMs = "fetch.timeout.ms";
    public const int DefaultFetchTimeoutMs = 5000;

    // JetStream Producer
    public const string PublishTimeoutMs = "publish.timeout.ms";
    public const int DefaultPublishTimeoutMs = 30000;
    public const string Retries = "retries";
    public const int DefaultRetries = 3;

    // Reconnection
    public const string ReconnectWaitMs = "reconnect.wait.ms";
    public const int DefaultReconnectWaitMs = 2000;
    public const string MaxReconnects = "max.reconnects";
    public const int DefaultMaxReconnects = -1; // Unlimited

    // Offset tracking
    public const string OffsetStreamSequence = "stream_sequence";
    public const string OffsetConsumerSequence = "consumer_sequence";
}
