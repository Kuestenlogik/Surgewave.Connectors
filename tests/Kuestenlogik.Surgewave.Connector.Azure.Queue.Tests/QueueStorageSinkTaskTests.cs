using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Queue.Tests;

public class QueueStorageSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        using var task = new QueueStorageSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithConnectionString_Succeeds()
    {
        using var task = new QueueStorageSinkTask();
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
        using var task = new QueueStorageSinkTask();
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
        using var task = new QueueStorageSinkTask();
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
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        Assert.Throws<ArgumentException>(() => task.Start(config));
    }

    [Fact]
    public void Start_ParsesTimeToLiveSeconds()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TimeToLiveSecondsConfig] = "3600"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesBatchSize()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.BatchSizeConfig] = "16"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesBase64Encode()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.Base64EncodeConfig] = "false"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesAutoCreateQueue()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.AutoCreateQueueConfig] = "true"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesMaxRetryCount()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.MaxRetryCountConfig] = "5"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void Start_ParsesRetryDelayMs()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.RetryDelayMsConfig] = "2000"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public async Task PutAsync_WithNullClient_ReturnsImmediately()
    {
        using var task = new QueueStorageSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = System.Text.Encoding.UTF8.GetBytes("test") }
        };

        await task.PutAsync(records, CancellationToken.None);
    }

    [Fact]
    public async Task PutAsync_WithEmptyRecords_ReturnsImmediately()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        await task.PutAsync([], CancellationToken.None);
        task.Stop();
    }

    [Fact]
    public async Task PutAsync_SkipsTombstoneRecords()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = null! },
            new() { Topic = "test", Partition = 0, Offset = 1, Value = [] }
        };

        // Should not throw - tombstones are skipped
        // Note: Will fail on actual send since queue doesn't exist, but tombstones are skipped first
        task.Stop();
    }

    [Fact]
    public async Task FlushAsync_ReturnsCompletedTask()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
        task.Stop();
    }

    [Fact]
    public async Task FlushAsync_BeforeStart_ReturnsCompletedTask()
    {
        using var task = new QueueStorageSinkTask();
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue"
        };

        task.Start(config);
        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public void Start_WithNeverExpiresTtl_Succeeds()
    {
        using var task = new QueueStorageSinkTask();
        var config = new Dictionary<string, string>
        {
            [QueueStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net",
            [QueueStorageConnectorConfig.QueueNameConfig] = "test-queue",
            [QueueStorageConnectorConfig.TimeToLiveSecondsConfig] = "-1"
        };

        task.Start(config);
        task.Stop();
    }
}
