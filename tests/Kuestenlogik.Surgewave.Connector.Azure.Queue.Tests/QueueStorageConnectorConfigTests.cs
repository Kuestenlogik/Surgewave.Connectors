namespace Kuestenlogik.Surgewave.Connector.Azure.Queue.Tests;

public class QueueStorageConnectorConfigTests
{
    [Fact]
    public void ConnectionStringConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.connection.string", QueueStorageConnectorConfig.ConnectionStringConfig);
    }

    [Fact]
    public void AccountNameConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.account.name", QueueStorageConnectorConfig.AccountNameConfig);
    }

    [Fact]
    public void AccountKeyConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.account.key", QueueStorageConnectorConfig.AccountKeyConfig);
    }

    [Fact]
    public void EndpointConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.endpoint", QueueStorageConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void QueueNameConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.name", QueueStorageConnectorConfig.QueueNameConfig);
    }

    [Fact]
    public void TopicsConfig_HasExpectedValue()
    {
        Assert.Equal("topics", QueueStorageConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void PollIntervalMsConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.poll.interval.ms", QueueStorageConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void MaxMessagesPerPollConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.max.messages.per.poll", QueueStorageConnectorConfig.MaxMessagesPerPollConfig);
    }

    [Fact]
    public void VisibilityTimeoutSecondsConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.visibility.timeout.seconds", QueueStorageConnectorConfig.VisibilityTimeoutSecondsConfig);
    }

    [Fact]
    public void TopicPatternConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.topic.pattern", QueueStorageConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void IncludeMetadataConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.include.metadata", QueueStorageConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void DeleteAfterReadConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.delete.after.read", QueueStorageConnectorConfig.DeleteAfterReadConfig);
    }

    [Fact]
    public void Base64DecodeConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.base64.decode", QueueStorageConnectorConfig.Base64DecodeConfig);
    }

    [Fact]
    public void TimeToLiveSecondsConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.time.to.live.seconds", QueueStorageConnectorConfig.TimeToLiveSecondsConfig);
    }

    [Fact]
    public void BatchSizeConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.batch.size", QueueStorageConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Base64EncodeConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.base64.encode", QueueStorageConnectorConfig.Base64EncodeConfig);
    }

    [Fact]
    public void MaxRetryCountConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.max.retry.count", QueueStorageConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void RetryDelayMsConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.retry.delay.ms", QueueStorageConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void AutoCreateQueueConfig_HasExpectedValue()
    {
        Assert.Equal("azure.queue.auto.create", QueueStorageConnectorConfig.AutoCreateQueueConfig);
    }

    [Fact]
    public void DefaultPollIntervalMs_HasExpectedValue()
    {
        Assert.Equal(1000L, QueueStorageConnectorConfig.DefaultPollIntervalMs);
    }

    [Fact]
    public void DefaultMaxMessagesPerPoll_HasExpectedValue()
    {
        Assert.Equal(32, QueueStorageConnectorConfig.DefaultMaxMessagesPerPoll);
    }

    [Fact]
    public void DefaultVisibilityTimeoutSeconds_HasExpectedValue()
    {
        Assert.Equal(30, QueueStorageConnectorConfig.DefaultVisibilityTimeoutSeconds);
    }

    [Fact]
    public void DefaultTimeToLiveSeconds_HasExpectedValue()
    {
        Assert.Equal(-1, QueueStorageConnectorConfig.DefaultTimeToLiveSeconds);
    }

    [Fact]
    public void DefaultBatchSize_HasExpectedValue()
    {
        Assert.Equal(32, QueueStorageConnectorConfig.DefaultBatchSize);
    }

    [Fact]
    public void DefaultMaxRetryCount_HasExpectedValue()
    {
        Assert.Equal(3, QueueStorageConnectorConfig.DefaultMaxRetryCount);
    }

    [Fact]
    public void DefaultRetryDelayMs_HasExpectedValue()
    {
        Assert.Equal(1000L, QueueStorageConnectorConfig.DefaultRetryDelayMs);
    }

    [Fact]
    public void DefaultTopicPattern_HasExpectedValue()
    {
        Assert.Equal("queue.${queue}", QueueStorageConnectorConfig.DefaultTopicPattern);
    }

    [Fact]
    public void OffsetMessageId_HasExpectedValue()
    {
        Assert.Equal("message_id", QueueStorageConnectorConfig.OffsetMessageId);
    }

    [Fact]
    public void OffsetPopReceipt_HasExpectedValue()
    {
        Assert.Equal("pop_receipt", QueueStorageConnectorConfig.OffsetPopReceipt);
    }

    [Fact]
    public void OffsetDequeueCount_HasExpectedValue()
    {
        Assert.Equal("dequeue_count", QueueStorageConnectorConfig.OffsetDequeueCount);
    }

    [Fact]
    public void HeaderQueueName_HasExpectedValue()
    {
        Assert.Equal("queue.name", QueueStorageConnectorConfig.HeaderQueueName);
    }

    [Fact]
    public void HeaderMessageId_HasExpectedValue()
    {
        Assert.Equal("queue.message.id", QueueStorageConnectorConfig.HeaderMessageId);
    }

    [Fact]
    public void HeaderPopReceipt_HasExpectedValue()
    {
        Assert.Equal("queue.pop.receipt", QueueStorageConnectorConfig.HeaderPopReceipt);
    }

    [Fact]
    public void HeaderDequeueCount_HasExpectedValue()
    {
        Assert.Equal("queue.dequeue.count", QueueStorageConnectorConfig.HeaderDequeueCount);
    }

    [Fact]
    public void HeaderInsertedOn_HasExpectedValue()
    {
        Assert.Equal("queue.inserted.on", QueueStorageConnectorConfig.HeaderInsertedOn);
    }

    [Fact]
    public void HeaderExpiresOn_HasExpectedValue()
    {
        Assert.Equal("queue.expires.on", QueueStorageConnectorConfig.HeaderExpiresOn);
    }

    [Fact]
    public void HeaderNextVisibleOn_HasExpectedValue()
    {
        Assert.Equal("queue.next.visible.on", QueueStorageConnectorConfig.HeaderNextVisibleOn);
    }
}
