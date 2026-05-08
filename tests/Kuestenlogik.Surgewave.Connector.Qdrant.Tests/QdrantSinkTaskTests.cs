namespace Kuestenlogik.Surgewave.Connector.Qdrant.Tests;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class QdrantSinkTaskTests
{
    [Fact]
    public void QdrantSinkTask_HasCorrectVersion()
    {
        using var task = new QdrantSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void QdrantSinkTask_Start_InitializesWithValidConfig()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.VectorFieldConfig] = "embedding"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void QdrantSinkTask_Start_InitializesWithApiKey()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.ApiKeyConfig] = "test-api-key"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void QdrantSinkTask_Start_InitializesWithHttps()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "qdrant.example.com",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.HttpsConfig] = "true",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public async Task QdrantSinkTask_PutAsync_BuffersRecords()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.BatchSizeConfig] = "100" // High batch size so it won't flush
        };

        task.Start(config);

        // Create a record with vector data
        var vector = new float[1536];
        for (int i = 0; i < vector.Length; i++) vector[i] = 0.1f;

        var recordData = new
        {
            embedding = vector,
            text = "Hello world",
            metadata = new { source = "test" }
        };

        var records = new List<SinkRecord>
        {
            CreateSinkRecord("test-topic", 0, 0, JsonSerializer.Serialize(recordData))
        };

        // Should not throw - just buffers without upserting
        await task.PutAsync(records, CancellationToken.None);

        task.Stop();
    }

    [Fact]
    public async Task QdrantSinkTask_FlushAsync_HandlesEmptyBuffer()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection"
        };

        task.Start(config);

        // Should not throw with empty buffer
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        task.Stop();
    }

    [Theory]
    [InlineData(QdrantConnectorConfig.DistanceCosine)]
    [InlineData(QdrantConnectorConfig.DistanceEuclid)]
    [InlineData(QdrantConnectorConfig.DistanceDot)]
    public void QdrantSinkTask_Start_ParsesDistanceMetric(string metric)
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.DistanceMetricConfig] = metric
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Theory]
    [InlineData(QdrantConnectorConfig.IdStrategyAuto)]
    [InlineData(QdrantConnectorConfig.IdStrategyKey)]
    public void QdrantSinkTask_Start_ParsesIdStrategy(string strategy)
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.IdStrategyConfig] = strategy
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void QdrantSinkTask_Start_ParsesFieldIdStrategy()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.IdStrategyConfig] = QdrantConnectorConfig.IdStrategyField,
            [QdrantConnectorConfig.IdFieldConfig] = "document_id"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void QdrantSinkTask_Start_ParsesPayloadFields()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.PayloadFieldsConfig] = "title, author, timestamp"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void QdrantSinkTask_Start_ParsesBatchConfig()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.BatchSizeConfig] = "50",
            [QdrantConnectorConfig.RetryMaxConfig] = "5",
            [QdrantConnectorConfig.RetryBackoffMsConfig] = "2000"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void QdrantSinkTask_Start_ParsesVectorConfig()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection",
            [QdrantConnectorConfig.VectorFieldConfig] = "vector",
            [QdrantConnectorConfig.VectorSizeConfig] = "3072",
            [QdrantConnectorConfig.CreateCollectionConfig] = "false"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void QdrantSinkTask_Stop_ClearsState()
    {
        using var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection"
        };

        task.Start(config);
        task.Stop();

        // Multiple stops should be safe
        task.Stop();
    }

    [Fact]
    public void QdrantSinkTask_Dispose_DisposesClient()
    {
        var task = new QdrantSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [QdrantConnectorConfig.HostConfig] = "localhost",
            [QdrantConnectorConfig.PortConfig] = "6334",
            [QdrantConnectorConfig.CollectionConfig] = "test-collection"
        };

        task.Start(config);
        task.Dispose();

        // Multiple disposes should be safe
        task.Dispose();
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }

    private static SinkRecord CreateSinkRecord(string topic, int partition, long offset, string value, string? key = null)
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = partition,
            Offset = offset,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = Encoding.UTF8.GetBytes(value),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>()
        };
    }
}
