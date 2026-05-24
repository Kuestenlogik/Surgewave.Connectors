using Kuestenlogik.Surgewave.Connector.Gcp.Firestore;

namespace Kuestenlogik.Surgewave.Connector.Gcp.Firestore.Tests;

public class FirestoreConnectorConfigTests
{
    [Fact]
    public void ConnectionSettings_HaveExpectedValues()
    {
        Assert.Equal("gcp.firestore.project.id", FirestoreConnectorConfig.ProjectIdConfig);
        Assert.Equal("gcp.firestore.credentials.json", FirestoreConnectorConfig.CredentialsJsonConfig);
        Assert.Equal("gcp.firestore.credentials.file", FirestoreConnectorConfig.CredentialsFileConfig);
        Assert.Equal("gcp.firestore.emulator.host", FirestoreConnectorConfig.EmulatorHostConfig);
    }

    [Fact]
    public void CollectionSettings_HaveExpectedValues()
    {
        Assert.Equal("gcp.firestore.collection", FirestoreConnectorConfig.CollectionPathConfig);
        Assert.Equal("gcp.firestore.document.id.field", FirestoreConnectorConfig.DocumentIdFieldConfig);
    }

    [Fact]
    public void SourceSettings_HaveExpectedValues()
    {
        Assert.Equal("gcp.firestore.topic.pattern", FirestoreConnectorConfig.TopicPatternConfig);
        Assert.Equal("gcp.firestore.poll.interval.ms", FirestoreConnectorConfig.PollIntervalMsConfig);
        Assert.Equal("gcp.firestore.max.documents.per.poll", FirestoreConnectorConfig.MaxDocumentsPerPollConfig);
        Assert.Equal("gcp.firestore.include.metadata", FirestoreConnectorConfig.IncludeMetadataConfig);
        Assert.Equal("gcp.firestore.watch.mode", FirestoreConnectorConfig.WatchModeConfig);
        Assert.Equal("gcp.firestore.query.filter", FirestoreConnectorConfig.QueryFilterConfig);
        Assert.Equal("gcp.firestore.order.by", FirestoreConnectorConfig.OrderByFieldConfig);
        Assert.Equal("gcp.firestore.order.direction", FirestoreConnectorConfig.OrderDirectionConfig);
        Assert.Equal("gcp.firestore.timestamp.field", FirestoreConnectorConfig.TimestampFieldConfig);
    }

    [Fact]
    public void SinkSettings_HaveExpectedValues()
    {
        Assert.Equal("topics", FirestoreConnectorConfig.TopicsConfig);
        Assert.Equal("gcp.firestore.write.mode", FirestoreConnectorConfig.WriteModeConfig);
        Assert.Equal("gcp.firestore.batch.size", FirestoreConnectorConfig.BatchSizeConfig);
        Assert.Equal("gcp.firestore.max.retry.count", FirestoreConnectorConfig.MaxRetryCountConfig);
        Assert.Equal("gcp.firestore.retry.delay.ms", FirestoreConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Defaults_HaveExpectedValues()
    {
        Assert.Equal("firestore.${collection}", FirestoreConnectorConfig.DefaultTopicPattern);
        Assert.Equal(5000L, FirestoreConnectorConfig.DefaultPollIntervalMs);
        Assert.Equal(500, FirestoreConnectorConfig.DefaultMaxDocumentsPerPoll);
        Assert.Equal("listen", FirestoreConnectorConfig.DefaultWatchMode);
        Assert.Equal("set", FirestoreConnectorConfig.DefaultWriteMode);
        Assert.Equal("asc", FirestoreConnectorConfig.DefaultOrderDirection);
        Assert.Equal(500, FirestoreConnectorConfig.DefaultBatchSize);
        Assert.Equal(3, FirestoreConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, FirestoreConnectorConfig.DefaultRetryDelayMs);
    }

    [Fact]
    public void HeaderNames_HaveExpectedValues()
    {
        Assert.Equal("firestore.project.id", FirestoreConnectorConfig.HeaderProjectId);
        Assert.Equal("firestore.collection", FirestoreConnectorConfig.HeaderCollectionPath);
        Assert.Equal("firestore.document.id", FirestoreConnectorConfig.HeaderDocumentId);
        Assert.Equal("firestore.document.path", FirestoreConnectorConfig.HeaderDocumentPath);
        Assert.Equal("firestore.update.time", FirestoreConnectorConfig.HeaderUpdateTime);
        Assert.Equal("firestore.create.time", FirestoreConnectorConfig.HeaderCreateTime);
        Assert.Equal("firestore.change.type", FirestoreConnectorConfig.HeaderChangeType);
    }

    [Fact]
    public void OffsetTrackingKeys_HaveExpectedValues()
    {
        Assert.Equal("document_id", FirestoreConnectorConfig.OffsetDocumentId);
        Assert.Equal("update_time", FirestoreConnectorConfig.OffsetUpdateTime);
        Assert.Equal("collection_path", FirestoreConnectorConfig.OffsetCollectionPath);
    }

    [Fact]
    public void DefaultWatchMode_IsListen()
    {
        Assert.Equal("listen", FirestoreConnectorConfig.DefaultWatchMode);
    }

    [Fact]
    public void DefaultWriteMode_IsSet()
    {
        Assert.Equal("set", FirestoreConnectorConfig.DefaultWriteMode);
    }

    [Fact]
    public void DefaultPollInterval_IsReasonable()
    {
        Assert.Equal(5000L, FirestoreConnectorConfig.DefaultPollIntervalMs);
        Assert.True(FirestoreConnectorConfig.DefaultPollIntervalMs >= 1000);
    }

    [Fact]
    public void DefaultBatchSize_IsReasonable()
    {
        Assert.Equal(500, FirestoreConnectorConfig.DefaultBatchSize);
        Assert.True(FirestoreConnectorConfig.DefaultBatchSize <= 500); // Firestore limit
    }

    [Fact]
    public void DefaultMaxDocumentsPerPoll_IsReasonable()
    {
        Assert.Equal(500, FirestoreConnectorConfig.DefaultMaxDocumentsPerPoll);
    }

    [Fact]
    public void DefaultRetrySettings_AreReasonable()
    {
        Assert.Equal(3, FirestoreConnectorConfig.DefaultMaxRetryCount);
        Assert.Equal(1000L, FirestoreConnectorConfig.DefaultRetryDelayMs);
    }
}
