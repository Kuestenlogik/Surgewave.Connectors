using Xunit;
using Kuestenlogik.Surgewave.Connector.Aws.DynamoDB;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.DynamoDB.Tests;

public class DynamoDbSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new DynamoDbSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsDynamoDbSinkTask()
    {
        var connector = new DynamoDbSinkConnector();
        Assert.Equal(typeof(DynamoDbSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredDefinitions()
    {
        var connector = new DynamoDbSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.TableNameConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.TopicsConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.PartitionKeyFieldConfig);
    }

    [Fact]
    public void Config_ContainsOptionalDefinitions()
    {
        var connector = new DynamoDbSinkConnector();

        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.RegionConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.AccessKeyConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.SecretKeyConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.EndpointConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.WriteModeConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.SortKeyFieldConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.BatchSizeConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.AutoCreateTableConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.BillingModeConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.ReadCapacityConfig);
        Assert.Contains(connector.Config.Keys, k => k.Name == DynamoDbConnectorConfig.WriteCapacityConfig);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithMissingTableName_ThrowsArgumentException()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyTableName_ThrowsArgumentException()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingTopics_ThrowsArgumentException()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyTopics_ThrowsArgumentException()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingPartitionKey_ThrowsArgumentException()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptyPartitionKey_ThrowsArgumentException()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = ""
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithSortKey_Succeeds()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "pk",
            [DynamoDbConnectorConfig.SortKeyFieldConfig] = "sk"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWriteModePut_Succeeds()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.WriteModeConfig] = DynamoDbConnectorConfig.WriteModePut
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWriteModeInsert_Succeeds()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.WriteModeConfig] = DynamoDbConnectorConfig.WriteModeInsert
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWriteModeUpdate_Succeeds()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.WriteModeConfig] = DynamoDbConnectorConfig.WriteModeUpdate
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithWriteModeDelete_Succeeds()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.WriteModeConfig] = DynamoDbConnectorConfig.WriteModeDelete
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithAutoCreateTable_Succeeds()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.AutoCreateTableConfig] = "true"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBillingModeProvisioned_Succeeds()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.BillingModeConfig] = DynamoDbConnectorConfig.BillingModeProvisioned,
            [DynamoDbConnectorConfig.ReadCapacityConfig] = "10",
            [DynamoDbConnectorConfig.WriteCapacityConfig] = "10"
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void Start_WithBillingModePayPerRequest_Succeeds()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.BillingModeConfig] = DynamoDbConnectorConfig.BillingModePayPerRequest
        };

        var exception = Record.Exception(() => connector.Start(config));

        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id",
            [DynamoDbConnectorConfig.BatchSizeConfig] = "10"
        };
        connector.Start(config);

        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal("10", taskConfigs[0][DynamoDbConnectorConfig.BatchSizeConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new DynamoDbSinkConnector();
        var config = new Dictionary<string, string>
        {
            [DynamoDbConnectorConfig.TableNameConfig] = "TestTable",
            [DynamoDbConnectorConfig.TopicsConfig] = "test-topic",
            [DynamoDbConnectorConfig.PartitionKeyFieldConfig] = "id"
        };
        connector.Start(config);

        var exception = Record.Exception(() =>
        {
            connector.Stop();
            connector.Stop();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Config_HasDefaultRegion()
    {
        var connector = new DynamoDbSinkConnector();
        var regionKey = connector.Config.Keys.First(k => k.Name == DynamoDbConnectorConfig.RegionConfig);

        Assert.Equal(DynamoDbConnectorConfig.DefaultRegion, regionKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultWriteMode()
    {
        var connector = new DynamoDbSinkConnector();
        var writeModeKey = connector.Config.Keys.First(k => k.Name == DynamoDbConnectorConfig.WriteModeConfig);

        Assert.Equal(DynamoDbConnectorConfig.WriteModePut, writeModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultBatchSize()
    {
        var connector = new DynamoDbSinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == DynamoDbConnectorConfig.BatchSizeConfig);

        Assert.Equal(DynamoDbConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasDefaultBillingMode()
    {
        var connector = new DynamoDbSinkConnector();
        var billingModeKey = connector.Config.Keys.First(k => k.Name == DynamoDbConnectorConfig.BillingModeConfig);

        Assert.Equal(DynamoDbConnectorConfig.BillingModePayPerRequest, billingModeKey.DefaultValue);
    }
}
