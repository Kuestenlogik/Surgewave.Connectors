namespace Kuestenlogik.Surgewave.Connector.MongoDB;

/// <summary>
/// Configuration constants for MongoDB connectors.
/// </summary>
public static class MongoDbConnectorConfig
{
    // Connection
    public const string ConnectionString = "mongodb.connection";
    public const string Database = "mongodb.database";
    public const string Collection = "mongodb.collection";

    // Source mode
    public const string SourceMode = "source.mode";
    public const string SourceModeChangeStream = "change_stream";
    public const string SourceModePoll = "poll";

    // Topic configuration
    public const string TopicPrefix = "topic.prefix";
    public const string TopicPattern = "topic.pattern";
    public const string DefaultTopicPattern = "${database}.${collection}";

    // Change stream options
    public const string ChangeStreamFullDocument = "change.stream.full.document";
    public const string FullDocumentDefault = "default";
    public const string FullDocumentUpdateLookup = "updateLookup";
    public const string FullDocumentWhenAvailable = "whenAvailable";

    // Polling options
    public const string PollField = "poll.field";
    public const string DefaultPollField = "_id";
    public const string PollIntervalMs = "poll.interval.ms";
    public const long DefaultPollIntervalMs = 1000;

    // Batch configuration
    public const string BatchMaxRecords = "batch.max.records";
    public const int DefaultBatchMaxRecords = 500;

    // Pipeline
    public const string Pipeline = "pipeline";

    // Sink-specific
    public const string Topics = "topics";
    public const string WriteMode = "write.mode";
    public const string WriteModeInsert = "insert";
    public const string WriteModeUpsert = "upsert";
    public const string WriteModeReplace = "replace";

    // Document ID strategy
    public const string DocumentIdStrategy = "document.id.strategy";
    public const string DocumentIdStrategyAuto = "auto";
    public const string DocumentIdStrategyKey = "key";
    public const string DocumentIdStrategyField = "field";
    public const string DocumentIdField = "document.id.field";
    public const string DefaultDocumentIdField = "_id";

    // Write concern
    public const string WriteConcern = "write.concern";
    public const string WriteConcernW1 = "w1";
    public const string WriteConcernMajority = "majority";
    public const string WriteConcernUnacknowledged = "unacknowledged";

    // Sink batch size
    public const string BatchSize = "batch.size";
    public const int DefaultBatchSize = 100;

    // Retry configuration
    public const string RetryMax = "retry.max";
    public const int DefaultRetryMax = 3;
    public const string RetryBackoffMs = "retry.backoff.ms";
    public const long DefaultRetryBackoffMs = 1000;

    // Source offset keys
    public const string OffsetResumeToken = "resumeToken";
    public const string OffsetLastPolledValue = "lastPolledValue";

    // CDC operation types
    public const string OpCreate = "c";
    public const string OpUpdate = "u";
    public const string OpReplace = "r";
    public const string OpDelete = "d";
}
