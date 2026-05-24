namespace Kuestenlogik.Surgewave.Connector.Nats.Kv;

/// <summary>
/// Configuration constants for NATS Key-Value store connectors.
/// </summary>
public static class NatsKvConnectorConfig
{
    // Connection
    public const string Url = "nats.url";
    public const string DefaultUrl = "nats://localhost:4222";
    public const string CredentialsFile = "nats.credentials.file";
    public const string Token = "nats.token";
    public const string Username = "nats.username";
    public const string Password = "nats.password";

    // TLS
    public const string TlsEnabled = "tls.enabled";
    public const bool DefaultTlsEnabled = false;
    public const string TlsCertificatePath = "tls.certificate.path";
    public const string TlsKeyPath = "tls.key.path";
    public const string TlsCaPath = "tls.ca.path";
    public const string TlsValidateCertificate = "tls.validate.certificate";
    public const bool DefaultTlsValidateCertificate = true;

    // Topics
    public const string Topic = "topic";
    public const string Topics = "topics";

    // KV Store
    public const string Bucket = "kv.bucket";
    public const string KeyPattern = "kv.key.pattern";
    public const string DefaultKeyPattern = "*"; // Watch all keys

    // KV Store Operations
    public const string CreateBucketIfMissing = "kv.create.bucket";
    public const bool DefaultCreateBucketIfMissing = true;
    public const string MaxBucketSize = "kv.bucket.max.size";
    public const long DefaultMaxBucketSize = -1; // Unlimited
    public const string MaxValueSize = "kv.bucket.max.value.size";
    public const int DefaultMaxValueSize = -1; // Unlimited
    public const string History = "kv.bucket.history";
    public const int DefaultHistory = 1;
    public const string Ttl = "kv.bucket.ttl.seconds";
    public const int DefaultTtl = 0; // No TTL
    public const string Replicas = "kv.bucket.replicas";
    public const int DefaultReplicas = 1;

    // Source specific
    public const string WatchMode = "kv.watch.mode";
    public const string WatchAll = "all";
    public const string WatchUpdatesOnly = "updates";
    public const string DefaultWatchMode = WatchAll;
    public const string IncludeHistory = "kv.include.history";
    public const bool DefaultIncludeHistory = false;
    public const string PollIntervalMs = "kv.poll.interval.ms";
    public const int DefaultPollIntervalMs = 100;

    // Sink specific
    public const string KeyField = "kv.key.field";
    public const string DefaultKeyField = "key";
    public const string ValueField = "kv.value.field";
    public const string WriteMode = "kv.write.mode";
    public const string WriteModeCreate = "create";
    public const string WriteModeUpdate = "update";
    public const string WriteModeUpsert = "put";
    public const string WriteModeDelete = "delete";
    public const string DefaultWriteMode = WriteModeUpsert;

    // Reconnection
    public const string ReconnectWaitMs = "reconnect.wait.ms";
    public const int DefaultReconnectWaitMs = 2000;
    public const string MaxReconnects = "max.reconnects";
    public const int DefaultMaxReconnects = -1; // Unlimited
}
