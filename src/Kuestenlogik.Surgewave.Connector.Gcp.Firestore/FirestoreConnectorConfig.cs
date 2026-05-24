namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore;

/// <summary>
/// Configuration constants for Google Cloud Firestore connectors.
/// </summary>
public static class FirestoreConnectorConfig
{
    // Connection configs
    public const string ProjectIdConfig = "gcp.firestore.project.id";
    public const string CredentialsJsonConfig = "gcp.firestore.credentials.json";
    public const string CredentialsFileConfig = "gcp.firestore.credentials.file";
    public const string EmulatorHostConfig = "gcp.firestore.emulator.host";

    // Collection configs
    public const string CollectionPathConfig = "gcp.firestore.collection";
    public const string DocumentIdFieldConfig = "gcp.firestore.document.id.field";

    // Source configs
    public const string TopicPatternConfig = "gcp.firestore.topic.pattern";
    public const string PollIntervalMsConfig = "gcp.firestore.poll.interval.ms";
    public const string MaxDocumentsPerPollConfig = "gcp.firestore.max.documents.per.poll";
    public const string IncludeMetadataConfig = "gcp.firestore.include.metadata";
    public const string WatchModeConfig = "gcp.firestore.watch.mode"; // poll, listen
    public const string QueryFilterConfig = "gcp.firestore.query.filter";
    public const string OrderByFieldConfig = "gcp.firestore.order.by";
    public const string OrderDirectionConfig = "gcp.firestore.order.direction"; // asc, desc
    public const string TimestampFieldConfig = "gcp.firestore.timestamp.field";

    // Sink configs
    public const string TopicsConfig = "topics";
    public const string WriteModeConfig = "gcp.firestore.write.mode"; // set, create, update, merge
    public const string BatchSizeConfig = "gcp.firestore.batch.size";
    public const string MaxRetryCountConfig = "gcp.firestore.max.retry.count";
    public const string RetryDelayMsConfig = "gcp.firestore.retry.delay.ms";

    // Default values
    public const string DefaultTopicPattern = "firestore.${collection}";
    public const long DefaultPollIntervalMs = 5000;
    public const int DefaultMaxDocumentsPerPoll = 500;
    public const string DefaultWatchMode = "listen";
    public const string DefaultWriteMode = "set";
    public const string DefaultOrderDirection = "asc";
    public const int DefaultBatchSize = 500;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;

    // Header names
    public const string HeaderProjectId = "firestore.project.id";
    public const string HeaderCollectionPath = "firestore.collection";
    public const string HeaderDocumentId = "firestore.document.id";
    public const string HeaderDocumentPath = "firestore.document.path";
    public const string HeaderUpdateTime = "firestore.update.time";
    public const string HeaderCreateTime = "firestore.create.time";
    public const string HeaderChangeType = "firestore.change.type";

    // Offset tracking
    public const string OffsetDocumentId = "document_id";
    public const string OffsetUpdateTime = "update_time";
    public const string OffsetCollectionPath = "collection_path";
}
