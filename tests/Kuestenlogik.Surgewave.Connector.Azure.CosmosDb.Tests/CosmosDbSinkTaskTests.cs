using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb.Tests;

public class CosmosDbSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new CosmosDbSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_ThrowsWhenConnectionMissing()
    {
        using var task = new CosmosDbSinkTask();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb",
            [CosmosDbConnectorConfig.ContainerConfig] = "testcontainer"
        };

        Assert.Throws<ArgumentException>(() => task.Start(config));
    }

    [Fact]
    public void Start_ThrowsWhenDatabaseMissing()
    {
        using var task = new CosmosDbSinkTask();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test=="
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public void Start_ThrowsWhenContainerMissing()
    {
        using var task = new CosmosDbSinkTask();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public async Task PutAsync_ReturnsWhenEmpty()
    {
        using var task = new CosmosDbSinkTask();

        // Without Start, should return without error for empty list
        await task.PutAsync([], CancellationToken.None);
    }

    [Fact]
    public async Task FlushAsync_ReturnsCompletedTask()
    {
        using var task = new CosmosDbSinkTask();

        var offsets = new Dictionary<TopicPartition, long>();
        await task.FlushAsync(offsets, CancellationToken.None);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new CosmosDbSinkTask();

        // Should not throw
        task.Stop();
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new CosmosDbSinkTask();

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public void WriteModes_HaveCorrectValues()
    {
        Assert.Equal("upsert", CosmosDbConnectorConfig.WriteModeUpsert);
        Assert.Equal("create", CosmosDbConnectorConfig.WriteModeCreate);
        Assert.Equal("replace", CosmosDbConnectorConfig.WriteModeReplace);
        Assert.Equal("delete", CosmosDbConnectorConfig.WriteModeDelete);
    }

    [Fact]
    public void DefaultBatchSize_IsReasonable()
    {
        Assert.Equal(100, CosmosDbConnectorConfig.DefaultBatchSize);
    }

    [Fact]
    public void DefaultThroughput_Is400RU()
    {
        Assert.Equal(400, CosmosDbConnectorConfig.DefaultThroughput);
    }

    [Fact]
    public void DefaultRetryCount_IsReasonable()
    {
        Assert.Equal(9, CosmosDbConnectorConfig.DefaultMaxRetryCount);
    }

    [Fact]
    public void DefaultRetryWaitTime_Is30Seconds()
    {
        Assert.Equal(30000L, CosmosDbConnectorConfig.DefaultMaxRetryWaitTimeMs);
    }

    [Fact]
    public void IdFieldConfig_HasCorrectDefault()
    {
        Assert.Equal("azure.cosmosdb.id.field", CosmosDbConnectorConfig.IdFieldConfig);
    }

    [Fact]
    public void PartitionKeyPathConfig_HasCorrectDefault()
    {
        Assert.Equal("azure.cosmosdb.partition.key.path", CosmosDbConnectorConfig.PartitionKeyPathConfig);
    }
}
