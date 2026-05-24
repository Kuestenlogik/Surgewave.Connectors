namespace Kuestenlogik.Surgewave.Connector.Neo4j;

/// <summary>
/// Configuration constants for Neo4j connectors.
/// </summary>
public static class Neo4jConnectorConfig
{
    // Connection Settings
    public const string UriConfig = "neo4j.uri";
    public const string UsernameConfig = "neo4j.username";
    public const string PasswordConfig = "neo4j.password";
    public const string DatabaseConfig = "neo4j.database";
    public const string EncryptedConfig = "neo4j.encrypted";

    // Source Settings
    public const string QueryConfig = "neo4j.query";
    public const string TopicConfig = "neo4j.topic";
    public const string TopicPatternConfig = "neo4j.topic.pattern";
    public const string PollIntervalMsConfig = "neo4j.poll.interval.ms";
    public const string MaxRowsPerPollConfig = "neo4j.max.rows.per.poll";
    public const string IncludeMetadataConfig = "neo4j.include.metadata";
    public const string TimestampPropertyConfig = "neo4j.timestamp.property";
    public const string IdPropertyConfig = "neo4j.id.property";
    public const string LabelConfig = "neo4j.label";

    // Sink Settings
    public const string TopicsConfig = "topics";
    public const string WriteModeConfig = "neo4j.write.mode";
    public const string BatchSizeConfig = "neo4j.batch.size";
    public const string MaxRetryCountConfig = "neo4j.max.retry.count";
    public const string RetryDelayMsConfig = "neo4j.retry.delay.ms";
    public const string MergePropertiesConfig = "neo4j.merge.properties";
    public const string NodeLabelFieldConfig = "neo4j.node.label.field";
    public const string RelationshipTypeFieldConfig = "neo4j.relationship.type.field";
    public const string SourceNodeLabelConfig = "neo4j.source.node.label";
    public const string TargetNodeLabelConfig = "neo4j.target.node.label";
    public const string SourceIdPropertyConfig = "neo4j.source.id.property";
    public const string TargetIdPropertyConfig = "neo4j.target.id.property";
    public const string UnwindParameterConfig = "neo4j.unwind.parameter";
    public const string CustomCypherConfig = "neo4j.custom.cypher";

    // Defaults
    public const string DefaultDatabase = "neo4j";
    public const string DefaultTopicPattern = "neo4j.${database}.${label}";
    public const long DefaultPollIntervalMs = 10000;
    public const int DefaultMaxRowsPerPoll = 10000;
    public const string DefaultWriteMode = "merge";
    public const int DefaultBatchSize = 1000;
    public const int DefaultMaxRetryCount = 3;
    public const long DefaultRetryDelayMs = 1000;
    public const string DefaultUnwindParameter = "events";

    // Header Names
    public const string HeaderDatabase = "neo4j.database";
    public const string HeaderLabel = "neo4j.label";
    public const string HeaderNodeId = "neo4j.node.id";
    public const string HeaderElementId = "neo4j.element.id";
    public const string HeaderRelationshipType = "neo4j.relationship.type";

    // Offset Tracking Keys
    public const string OffsetTimestamp = "timestamp";
    public const string OffsetId = "id";
    public const string OffsetElementId = "element_id";
}
