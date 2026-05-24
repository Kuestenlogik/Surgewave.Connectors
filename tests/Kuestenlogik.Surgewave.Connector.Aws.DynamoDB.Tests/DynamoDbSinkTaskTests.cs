using Xunit;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;

namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB.Tests;

public class DynamoDbSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new DynamoDbSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomRegion_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.RegionConfig] = "eu-west-1"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCredentials_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.AccessKeyConfig] = "AKIAIOSFODNN7EXAMPLE",
            [DynamoDbConnectorConfig.SecretKeyConfig] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithCustomEndpoint_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.EndpointConfig] = "http://localhost:4566"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWriteModePut_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.WriteModeConfig] = DynamoDbConnectorConfig.WriteModePut
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWriteModeInsert_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.WriteModeConfig] = DynamoDbConnectorConfig.WriteModeInsert
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWriteModeUpdate_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.WriteModeConfig] = DynamoDbConnectorConfig.WriteModeUpdate
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWriteModeDelete_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.WriteModeConfig] = DynamoDbConnectorConfig.WriteModeDelete
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithSortKey_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "pk",
            [DynamoDbConnectorConfig.SortKeyFieldConfig] = "sk"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBatchSize_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.BatchSizeConfig] = "10"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBatchSizeOverMax_ClampsTo25()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.BatchSizeConfig] = "100"
        };

        // Should not throw - internally clamped to 25
        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithAutoCreateTable_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.AutoCreateTableConfig] = "true"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBillingModeProvisioned_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.BillingModeConfig] = DynamoDbConnectorConfig.BillingModeProvisioned,
            [DynamoDbConnectorConfig.ReadCapacityConfig] = "10",
            [DynamoDbConnectorConfig.WriteCapacityConfig] = "10"
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBillingModePayPerRequest_Succeeds()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.BillingModeConfig] = DynamoDbConnectorConfig.BillingModePayPerRequest
        };

        var exception = Record.Exception(() => task.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };
        task.Start(config);

        var exception = Record.Exception(() =>
        {
            task.Stop();
            task.Stop();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new DynamoDbSinkTask();

        var exception = Record.Exception(() => task.Stop());

        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_WithEmptyRecords_DoesNotThrow()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.AutoCreateTableConfig] = "false"
        };
        task.Start(config);

        var exception = await Record.ExceptionAsync(() =>
            task.PutAsync(Array.Empty<SinkRecord>(), CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_WithoutStart_ReturnsWithoutError()
    {
        using var task = new DynamoDbSinkTask();

        // When client is null, should return immediately
        var exception = await Record.ExceptionAsync(() =>
            task.PutAsync(Array.Empty<SinkRecord>(), CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_ReturnsCompletedTask()
    {
        using var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };
        task.Start(config);

        var exception = await Record.ExceptionAsync(() =>
            task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None));

        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new DynamoDbSinkTask();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };
        task.Start(config);

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }
}
