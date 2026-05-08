using Kuestenlogik.Surgewave.Connector.Neo4j;

namespace Kuestenlogik.Surgewave.Connector.Neo4j.Tests;

public class Neo4jConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("neo4j.uri", Neo4jConnectorConfig.UriConfig);
        Assert.Equal("neo4j.username", Neo4jConnectorConfig.UsernameConfig);
        Assert.Equal("neo4j.password", Neo4jConnectorConfig.PasswordConfig);
        Assert.Equal("neo4j.database", Neo4jConnectorConfig.DatabaseConfig);
        Assert.Equal("neo4j.encrypted", Neo4jConnectorConfig.EncryptedConfig);
    }

    [Fact]
    public void SourceSettings_HaveExpectedValues()
    {
        Assert.Equal("neo4j.query", Neo4jConnectorConfig.QueryConfig);
        Assert.Equal("neo4j.topic", Neo4jConnectorConfig.TopicConfig);
        Assert.Equal("neo4j.topic.pattern", Neo4jConnectorConfig.TopicPatternConfig);
        Assert.Equal("neo4j.poll.interval.ms", Neo4jConnectorConfig.PollIntervalMsConfig);
        Assert.Equal("neo4j.max.rows.per.poll", Neo4jConnectorConfig.MaxRowsPerPollConfig);
        Assert.Equal("neo4j.include.metadata", Neo4jConnectorConfig.IncludeMetadataConfig);
        Assert.Equal("neo4j.timestamp.property", Neo4jConnectorConfig.TimestampPropertyConfig);
        Assert.Equal("neo4j.id.property", Neo4jConnectorConfig.IdPropertyConfig);
        Assert.Equal("neo4j.label", Neo4jConnectorConfig.LabelConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("topics", Neo4jConnectorConfig.TopicsConfig);
        Assert.Equal("neo4j.write.mode", Neo4jConnectorConfig.WriteModeConfig);
        Assert.Equal("neo4j.batch.size", Neo4jConnectorConfig.BatchSizeConfig);
        Assert.Equal("neo4j.max.retry.count", Neo4jConnectorConfig.MaxRetryCountConfig);
        Assert.Equal("neo4j.retry.delay.ms", Neo4jConnectorConfig.RetryDelayMsConfig);
        Assert.Equal("neo4j.merge.properties", Neo4jConnectorConfig.MergePropertiesConfig);
        Assert.Equal("neo4j.node.label.field", Neo4jConnectorConfig.NodeLabelFieldConfig);
        Assert.Equal("neo4j.relationship.type.field", Neo4jConnectorConfig.RelationshipTypeFieldConfig);
        Assert.Equal("neo4j.custom.cypher", Neo4jConnectorConfig.CustomCypherConfig);
        Assert.Equal("neo4j.unwind.parameter", Neo4jConnectorConfig.UnwindParameterConfig);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("neo4j", Neo4jConnectorConfig.DefaultDatabase);
        Assert.Equal("neo4j.${database}.${label}", Neo4jConnectorConfig.DefaultTopicPattern);
        Assert.Equal(10000L, Neo4jConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(10000, Neo4jConnectorConfig.DefaultMaxRowsPerPoll);
        Assert.Equal("merge", Neo4jConnectorConfig.DefaultWriteMode);
        Assert.Equal(1000, Neo4jConnectorConfig.DefaultBatchSize);
        Assert.Equal(3, Neo4jConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, Neo4jConnectorConfig.DefaultRetryDelayMs);
        Assert.Equal("events", Neo4jConnectorConfig.DefaultUnwindParameter);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("neo4j.database", Neo4jConnectorConfig.HeaderDatabase);
        Assert.Equal("neo4j.label", Neo4jConnectorConfig.HeaderLabel);
        Assert.Equal("neo4j.node.id", Neo4jConnectorConfig.HeaderNodeId);
        Assert.Equal("neo4j.element.id", Neo4jConnectorConfig.HeaderElementId);
        Assert.Equal("neo4j.relationship.type", Neo4jConnectorConfig.HeaderRelationshipType);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("timestamp", Neo4jConnectorConfig.OffsetTimestamp);
        Assert.Equal("id", Neo4jConnectorConfig.OffsetId);
        Assert.Equal("element_id", Neo4jConnectorConfig.OffsetElementId);
    }

    [Fact]
    public void DefaultPollInterval_IsReasonable()
    {
        Assert.Equal(10000L, Neo4jConnectorConfig.DefaultPollIntervalMs);
        Assert.True(Neo4jConnectorConfig.DefaultPollIntervalMs >= 1000);
    }

    [Fact]
    public void DefaultBatchSize_IsReasonable()
    {
        Assert.Equal(1000, Neo4jConnectorConfig.DefaultBatchSize);
        Assert.True(Neo4jConnectorConfig.DefaultBatchSize > 0);
    }

    [Fact]
    public void DefaultRetrySettings_AreReasonable()
    {
        Assert.Equal(3, Neo4jConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, Neo4jConnectorConfig.DefaultRetryDelayMs);
        Assert.True(Neo4jConnectorConfig.DefaultMaxRetryCount > 0);
        Assert.True(Neo4jConnectorConfig.DefaultRetryDelayMs > 0);
    }
}
