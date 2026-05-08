using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.InfluxDB;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InfluxDB.Tests;

public class InfluxDBSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsInfluxDBSinkTask()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Equal(typeof(InfluxDBSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesUrlConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.UrlConfig);
    }

    [Fact]
    public void Config_DefinesTokenConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.TokenConfig);
    }

    [Fact]
    public void Config_DefinesOrgConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.OrgConfig);
    }

    [Fact]
    public void Config_DefinesBucketConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.BucketConfig);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesMeasurementConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.MeasurementConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesMaxRetryCountConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.MaxRetryCountConfig);
    }

    [Fact]
    public void Config_DefinesRetryDelayMsConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.RetryDelayMsConfig);
    }

    [Fact]
    public void Config_DefinesMeasurementFieldConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.MeasurementFieldConfig);
    }

    [Fact]
    public void Config_DefinesTimestampFieldConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.TimestampFieldConfig);
    }

    [Fact]
    public void Config_DefinesTagFieldsConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.TagFieldsConfig);
    }

    [Fact]
    public void Config_DefinesFieldFieldsConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.FieldFieldsConfig);
    }

    [Fact]
    public void Config_DefinesPrecisionConfig()
    {
        var connector = new InfluxDBSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.PrecisionConfig);
    }

    [Fact]
    public void Start_ThrowsWhenUrlMissing()
    {
        var connector = new InfluxDBSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.UrlConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTokenMissing()
    {
        var connector = new InfluxDBSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.TokenConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenOrgMissing()
    {
        var connector = new InfluxDBSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.OrgConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenBucketMissing()
    {
        var connector = new InfluxDBSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.BucketConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new InfluxDBSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var connector = new InfluxDBSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.TopicsConfig] = "test-topic"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new InfluxDBSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[InfluxDBConnectorConfig.UrlConfig], taskConfigs[0][InfluxDBConnectorConfig.UrlConfig]);
        Assert.Equal(config[InfluxDBConnectorConfig.BucketConfig], taskConfigs[0][InfluxDBConnectorConfig.BucketConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new InfluxDBSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.TopicsConfig] = "test-topic"
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
    public void Config_TokenIsPasswordType()
    {
        var connector = new InfluxDBSinkConnector();
        var tokenKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.TokenConfig);
        Assert.Equal(ConfigType.Password, tokenKey.Type);
    }

    [Fact]
    public void Config_UrlIsHighImportance()
    {
        var connector = new InfluxDBSinkConnector();
        var urlKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.UrlConfig);
        Assert.Equal(Importance.High, urlKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedBatchSizeDefault()
    {
        var connector = new InfluxDBSinkConnector();
        var batchSizeKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.BatchSizeConfig);
        Assert.Equal(InfluxDBConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxRetryCountDefault()
    {
        var connector = new InfluxDBSinkConnector();
        var maxRetryKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.MaxRetryCountConfig);
        Assert.Equal(InfluxDBConnectorConfig.DefaultMaxRetryCount, maxRetryKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPrecisionDefault()
    {
        var connector = new InfluxDBSinkConnector();
        var precisionKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.PrecisionConfig);
        Assert.Equal(InfluxDBConnectorConfig.DefaultPrecision, precisionKey.DefaultValue);
    }
}

public class InfluxDBSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new InfluxDBSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PutAsync_SkipsNullValues()
    {
        using var task = new InfluxDBSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = null! }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_CompletesSuccessfully()
    {
        using var task = new InfluxDBSinkTask();
        var offsets = new Dictionary<TopicPartition, long>();

        var exception = await Record.ExceptionAsync(() => task.FlushAsync(offsets, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new InfluxDBSinkTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new InfluxDBSinkTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_HandlesInvalidJson()
    {
        using var task = new InfluxDBSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = System.Text.Encoding.UTF8.GetBytes("not valid json") }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_HandlesEmptyRecordsList()
    {
        using var task = new InfluxDBSinkTask();
        var records = new List<SinkRecord>();

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }
}
