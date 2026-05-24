namespace Kuestenlogik.Surgewave.Connector.Nats.ObjectStore;

/// <summary>
/// Configuration constants for NATS Object Store connector.
/// </summary>
public static class NatsObjectStoreConnectorConfig
{
    // Connection settings
    public const string Servers = "nats.servers";  // Comma-separated NATS servers
    public const string Username = "nats.username";
    public const string Password = "nats.password";
    public const string Token = "nats.token";
    public const string CredentialsFile = "nats.credentials.file";
    public const string TlsCertFile = "nats.tls.cert.file";
    public const string TlsKeyFile = "nats.tls.key.file";
    public const string TlsCaFile = "nats.tls.ca.file";

    // Object Store settings
    public const string BucketName = "nats.objectstore.bucket";
    public const string CreateBucket = "nats.objectstore.create.bucket";

    // Source settings
    public const string Topic = "topic";
    public const string WatchPrefix = "nats.objectstore.watch.prefix";  // Watch objects with prefix
    public const string IncludeHistory = "nats.objectstore.include.history";
    public const string IncludeDeletes = "nats.objectstore.include.deletes";
    public const string IncludeContent = "nats.objectstore.include.content";
    public const string MaxContentSize = "nats.objectstore.max.content.size";

    // Sink settings
    public const string Topics = "topics";
    public const string ObjectNameField = "nats.objectstore.name.field";
    public const string ObjectNamePrefix = "nats.objectstore.name.prefix";
    public const string ContentType = "nats.objectstore.content.type";
    public const string ChunkSize = "nats.objectstore.chunk.size";

    // Defaults
    public const string DefaultServer = "nats://localhost:4222";
    public const int DefaultMaxContentSize = 10 * 1024 * 1024;  // 10MB
    public const int DefaultChunkSize = 128 * 1024;  // 128KB
}
