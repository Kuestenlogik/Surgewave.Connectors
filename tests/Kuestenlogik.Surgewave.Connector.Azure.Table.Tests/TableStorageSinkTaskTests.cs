using Xunit;
using Kuestenlogik.Surgewave.Connector.Azure.Table;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Azure.Table.Tests;

public class TableStorageSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new TableStorageSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_ThrowsWhenConnectionMissing()
    {
        using var task = new TableStorageSinkTask();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.TableNameConfig] = "testtable"
        };

        Assert.Throws<ArgumentException>(() => task.Start(config));
    }

    [Fact]
    public void Start_ThrowsWhenTableNameMissing()
    {
        using var task = new TableStorageSinkTask();

        var config = new Dictionary<string, string>
        {
            [TableStorageConnectorConfig.ConnectionStringConfig] = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net"
        };

        Assert.Throws<KeyNotFoundException>(() => task.Start(config));
    }

    [Fact]
    public async Task PutAsync_ReturnsWhenEmpty()
    {
        using var task = new TableStorageSinkTask();

        // Without Start, should return without error for empty list
        await task.PutAsync([], CancellationToken.None);
    }

    [Fact]
    public async Task FlushAsync_ReturnsCompletedTask()
    {
        using var task = new TableStorageSinkTask();

        var offsets = new Dictionary<TopicPartition, long>();
        await task.FlushAsync(offsets, CancellationToken.None);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new TableStorageSinkTask();

        // Should not throw
        task.Stop();
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new TableStorageSinkTask();

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public void WriteModes_HaveCorrectValues()
    {
        Assert.Equal("upsert", TableStorageConnectorConfig.WriteModeUpsert);
        Assert.Equal("insert", TableStorageConnectorConfig.WriteModeInsert);
        Assert.Equal("update", TableStorageConnectorConfig.WriteModeUpdate);
        Assert.Equal("delete", TableStorageConnectorConfig.WriteModeDelete);
    }

    [Fact]
    public void DefaultBatchSize_IsReasonable()
    {
        Assert.Equal(100, TableStorageConnectorConfig.DefaultBatchSize);
    }

    [Fact]
    public void DefaultMaxRetryCount_IsReasonable()
    {
        Assert.Equal(3, TableStorageConnectorConfig.DefaultMaxRetryCount);
    }

    [Fact]
    public void DefaultRetryDelay_Is1Second()
    {
        Assert.Equal(1000L, TableStorageConnectorConfig.DefaultRetryDelayMs);
    }

    [Fact]
    public void PartitionKeyFieldConfig_HasCorrectValue()
    {
        Assert.Equal("azure.table.partition.key.field", TableStorageConnectorConfig.PartitionKeyFieldConfig);
    }

    [Fact]
    public void RowKeyFieldConfig_HasCorrectValue()
    {
        Assert.Equal("azure.table.row.key.field", TableStorageConnectorConfig.RowKeyFieldConfig);
    }
}
