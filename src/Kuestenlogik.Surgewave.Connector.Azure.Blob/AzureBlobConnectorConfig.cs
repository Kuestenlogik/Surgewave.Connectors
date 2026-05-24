namespace Kuestenlogik.Surgewave.Connector.Azure.Blob;

/// <summary>
/// Shared configuration constants for Azure Blob Storage connectors.
/// </summary>
internal static class AzureBlobConnectorConfig
{
    // Connection configs
    public const string ConnectionStringConfig = "azure.storage.connection.string";
    public const string AccountNameConfig = "azure.storage.account.name";
    public const string AccountKeyConfig = "azure.storage.account.key";
    public const string ContainerNameConfig = "azure.storage.container.name";
    public const string EndpointConfig = "azure.storage.endpoint";

    // Source-specific configs
    public const string PrefixConfig = "azure.blob.prefix";
    public const string TopicConfig = "topic";
    public const string FormatConfig = "format";
    public const string PollIntervalMsConfig = "poll.interval.ms";
    public const string DeleteAfterReadConfig = "delete.after.read";

    // Sink-specific configs
    public const string TopicsConfig = "topics";
    public const string PartitionerConfig = "partitioner";
    public const string FlushSizeConfig = "flush.size";
    public const string RotateIntervalMsConfig = "rotate.interval.ms";

    // Format options
    public const string FormatJson = "json";
    public const string FormatJsonLines = "jsonlines";
    public const string FormatCsv = "csv";
    public const string FormatRaw = "raw";
    public const string FormatParquet = "parquet";
    public const string FormatAvro = "avro";

    // Partitioner options
    public const string PartitionerDefault = "default";
    public const string PartitionerTime = "time";
    public const string PartitionerField = "field";

    // Default values
    public const string DefaultFormat = FormatJson;
    public const string DefaultPartitioner = PartitionerDefault;
    public const long DefaultPollIntervalMs = 10000;
    public const int DefaultFlushSize = 1000;
    public const long DefaultRotateIntervalMs = 3600000; // 1 hour
    public const bool DefaultDeleteAfterRead = false;
}
