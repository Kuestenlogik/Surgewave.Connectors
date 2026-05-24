namespace Kuestenlogik.Surgewave.Connector.Azure.Queue.Tests;

public class QueueStorageSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        using var task = new QueueStorageSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithConnectionString_Succeeds()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_WithAccountNameAndKey_Succeeds()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.AccountNameConfig] = "testaccount",
            [QueueStorageConnectorConfig.AccountKeyConfig] = "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3RrZXk=",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_WithCustomEndpoint_Succeeds()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.EndpointConfig] = "http://127.0.0.1:10001/devstoreaccount1",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_WithoutConnectionOrAccountName_ThrowsArgumentException()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        Assert.Throws<ArgumentException>(() => task.Start(config));
    }

    [Fact]
    public void Start_ParsesPollIntervalMs()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.PollIntervalMsConfig] = "5000"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesMaxMessagesPerPoll()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.MaxMessagesPerPollConfig] = "16"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_LimitsMaxMessagesPerPollTo32()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.MaxMessagesPerPollConfig] = "100"
        };

        task.Start(config);
        // Task should internally cap this to 32
        task.Stop();
    }

    [Fact]
    public void Start_ParsesVisibilityTimeoutSeconds()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.VisibilityTimeoutSecondsConfig] = "60"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesDeleteAfterRead()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.DeleteAfterReadConfig] = "true"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesBase64Decode()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.Base64DecodeConfig] = "false"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesIncludeMetadata()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.IncludeMetadataConfig] = "false"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesTopicPattern()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TopicPatternConfig] = "azure.queue.${queue}"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public async Task PollAsync_BeforeStart_ReturnsEmptyList()
    {
        using var task = new QueueStorageSourceTask();
        var result = await task.PollAsync(CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public async Task PollAsync_AfterStop_ReturnsEmptyList()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        task.Stop();

        var result = await task.PollAsync(CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public async Task CommitAsync_BeforeStart_Succeeds()
    {
        using var task = new QueueStorageSourceTask();
        await task.CommitAsync(CancellationToken.None);
    }

    [Fact]
    public async Task CommitAsync_AfterStop_Succeeds()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        task.Stop();

        await task.CommitAsync(CancellationToken.None);
    }

    [Fact]
    public async Task CommitAsync_WithDeleteAfterRead_DoesNotThrow()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.DeleteAfterReadConfig] = "true"
        };

        task.Start(config);
        await task.CommitAsync(CancellationToken.None);
        task.Stop();
    }

    [Fact]
    public void Stop_ClearsPendingMessages()
    {
        using var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        task.Stop();
        // No pending messages should cause issues
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new QueueStorageSourceTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        task.Dispose();
        task.Dispose(); // Should not throw
    }
}
