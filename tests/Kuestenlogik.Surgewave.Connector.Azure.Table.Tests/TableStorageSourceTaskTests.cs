using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.Table;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Table.Tests;

public class TableStorageSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new TableStorageSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_ThrowsWhenConnectionMissing()
    {
        using var task = new TableStorageSourceTask();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.TableNameConfig] = "testtable"
        };

        Assert.Throws<ArgumentException>(() => task.Start(config));
    }

    [Fact]
    public void Start_ThrowsWhenTableNameMissing()
    {
        using var task = new TableStorageSourceTask();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public async Task PollAsync_ReturnsEmptyWhenNotInitialized()
    {
        using var task = new TableStorageSourceTask();

        // Without Start, should return empty
        var records = await task.PollAsync(CancellationToken.None);
        Assert.Empty(records);
    }

    [Fact]
    public async Task CommitAsync_CompletesWithoutError()
    {
        using var task = new TableStorageSourceTask();

        // Should complete without error even when not started
        await task.CommitAsync(CancellationToken.None);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new TableStorageSourceTask();

        // Should not throw
        task.Stop();
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new TableStorageSourceTask();

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public void DefaultTopicPattern_UsesExpectedFormat()
    {
        Assert.Equal("table.${table}", TableStorageConnectorConfig.DefaultTopicPattern);
    }

    [Fact]
    public void DefaultPollInterval_IsReasonable()
    {
        Assert.Equal(5000L, TableStorageConnectorConfig.DefaultPollIntervalMs);
    }

    [Fact]
    public void DefaultMaxEntities_IsReasonable()
    {
        Assert.Equal(1000, TableStorageConnectorConfig.DefaultMaxEntitiesPerPoll);
    }

    [Fact]
    public void IncrementalModes_AreValid()
    {
        Assert.Equal("none", TableStorageConnectorConfig.IncrementalModeNone);
        Assert.Equal("timestamp", TableStorageConnectorConfig.IncrementalModeTimestamp);
        Assert.Equal("rowkey", TableStorageConnectorConfig.IncrementalModeRowKey);
    }
}
