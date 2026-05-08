namespace Kuestenlogik.Surgewave.Connector.PostgreSql;

/// <summary>
/// Shared configuration constants for PostgreSQL connectors.
/// </summary>
public static class PostgreSqlConnectorConfig
{
    // Common configs
    public const string ConnectionConfig = "postgresql.connection";

    // CDC Source configs
    public const string SlotNameConfig = "postgresql.slot.name";
    public const string PublicationNameConfig = "postgresql.publication.name";
    public const string TablesConfig = "postgresql.tables";
    public const string CreateSlotConfig = "postgresql.create.slot";
    public const string CreatePublicationConfig = "postgresql.create.publication";
    public const string TopicPrefixConfig = "topic.prefix";
    public const string TopicPatternConfig = "topic.pattern";
    public const string IncludeSchemaConfig = "include.schema";
    public const string IncludeBeforeValuesConfig = "include.before.values";
    public const string SnapshotModeConfig = "snapshot.mode";
    public const string PollIntervalMsConfig = "poll.interval.ms";
    public const string BatchMaxRecordsConfig = "batch.max.records";

    // Sink configs
    public const string TopicsConfig = "topics";
    public const string TableConfig = "postgresql.table";
    public const string SchemaConfig = "postgresql.schema";
    public const string InsertModeConfig = "insert.mode";
    public const string PkModeConfig = "pk.mode";
    public const string PkFieldsConfig = "pk.fields";
    public const string BatchSizeConfig = "batch.size";
    public const string RetryMaxConfig = "retry.max";
    public const string RetryBackoffMsConfig = "retry.backoff.ms";

    // pgvector configs
    public const string VectorFieldConfig = "vector.field";
    public const string VectorDimensionsConfig = "vector.dimensions";
    public const string VectorCreateExtensionConfig = "vector.create.extension";
    public const string VectorIndexTypeConfig = "vector.index.type";
    public const string VectorDistanceMetricConfig = "vector.distance.metric";

    // Default values - CDC Source
    public const string DefaultSlotName = "surgewave_cdc_slot";
    public const string DefaultPublicationName = "surgewave_publication";
    public const string DefaultTopicPattern = "${schema}.${table}";
    public const long DefaultPollIntervalMs = 100;
    public const int DefaultBatchMaxRecords = 500;

    // Default values - Sink
    public const string DefaultSchema = "public";
    public const int DefaultBatchSize = 100;
    public const int DefaultRetryMax = 3;
    public const long DefaultRetryBackoffMs = 1000;

    // Default values - pgvector
    public const int DefaultVectorDimensions = 1536; // OpenAI text-embedding-3-small default
    public const string VectorIndexNone = "none";
    public const string VectorIndexIvfflat = "ivfflat";
    public const string VectorIndexHnsw = "hnsw";
    public const string VectorDistanceCosine = "cosine";
    public const string VectorDistanceL2 = "l2";
    public const string VectorDistanceInnerProduct = "inner_product";

    // Insert modes
    public const string InsertModeInsert = "insert";
    public const string InsertModeUpsert = "upsert";

    // PK modes
    public const string PkModeRecordKey = "record_key";
    public const string PkModeRecordValue = "record_value";

    // Snapshot modes
    public const string SnapshotModeInitial = "initial";
    public const string SnapshotModeNever = "never";
    public const string SnapshotModeAlways = "always";
}
