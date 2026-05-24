using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.CosmosDb;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.CosmosDb.Tests;

public class CosmosDbSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new CosmosDbSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_ThrowsWhenConnectionMissing()
    {
        using var task = new CosmosDbSourceTask();

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
        using var task = new CosmosDbSourceTask();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test=="
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public void Start_ThrowsWhenContainerMissing()
    {
        using var task = new CosmosDbSourceTask();

        var config = new Dictionary<string, string>
        {
            [CosmosDbConnectorConfig.ConnectionStringConfig] = "AccountEndpoint=https://test.documents.azure.com:443/;AccountKey=test==",
            [CosmosDbConnectorConfig.DatabaseConfig] = "testdb"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public async Task PollAsync_ReturnsEmptyWhenNotInitialized()
    {
        using var task = new CosmosDbSourceTask();

        // Without Start, should return empty
        var records = await task.PollAsync(CancellationToken.None);
        Assert.Empty(records);
    }

    [Fact]
    public async Task CommitAsync_CompletesWithoutError()
    {
        using var task = new CosmosDbSourceTask();

        // Should complete without error even when not started
        await task.CommitAsync(CancellationToken.None);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new CosmosDbSourceTask();

        // Should not throw
        task.Stop();
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new CosmosDbSourceTask();

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public void DefaultTopicPattern_UsesExpectedFormat()
    {
        Assert.Equal("cosmosdb.${database}.${container}", CosmosDbConnectorConfig.DefaultTopicPattern);
    }

    [Fact]
    public void DefaultPollInterval_IsReasonable()
    {
        Assert.Equal(500L, CosmosDbConnectorConfig.DefaultPollIntervalMs);
    }

    [Fact]
    public void DefaultMaxItems_IsReasonable()
    {
        Assert.Equal(100, CosmosDbConnectorConfig.DefaultChangeFeedMaxItems);
    }

    [Fact]
    public void StartFromOptions_AreValid()
    {
        Assert.Equal("beginning", CosmosDbConnectorConfig.StartFromBeginning);
        Assert.Equal("now", CosmosDbConnectorConfig.StartFromNow);
        Assert.Equal("continuation", CosmosDbConnectorConfig.StartFromContinuation);
    }
}
