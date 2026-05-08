using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.InfluxDB;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InfluxDB.Tests;

public class InfluxDBSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsInfluxDBSourceTask()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Equal(typeof(InfluxDBSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesUrlConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.UrlConfig);
    }

    [Fact]
    public void Config_DefinesTokenConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.TokenConfig);
    }

    [Fact]
    public void Config_DefinesOrgConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.OrgConfig);
    }

    [Fact]
    public void Config_DefinesBucketConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.BucketConfig);
    }

    [Fact]
    public void Config_DefinesMeasurementConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.MeasurementConfig);
    }

    [Fact]
    public void Config_DefinesQueryConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.QueryConfig);
    }

    [Fact]
    public void Config_DefinesTopicPatternConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.TopicPatternConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesMaxRowsPerPollConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.MaxRowsPerPollConfig);
    }

    [Fact]
    public void Config_DefinesIncludeMetadataConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_DefinesTimeRangeConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.TimeRangeConfig);
    }

    [Fact]
    public void Config_DefinesStartTimeConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.StartTimeConfig);
    }

    [Fact]
    public void Config_DefinesStopTimeConfig()
    {
        var connector = new InfluxDBSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == InfluxDBConnectorConfig.StopTimeConfig);
    }

    [Fact]
    public void Start_ThrowsWhenUrlMissing()
    {
        var connector = new InfluxDBSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.MeasurementConfig] = "test-measurement"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.UrlConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenTokenMissing()
    {
        var connector = new InfluxDBSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.MeasurementConfig] = "test-measurement"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.TokenConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenOrgMissing()
    {
        var connector = new InfluxDBSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.MeasurementConfig] = "test-measurement"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.OrgConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenBucketMissing()
    {
        var connector = new InfluxDBSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.MeasurementConfig] = "test-measurement"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.BucketConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenMeasurementAndQueryBothMissing()
    {
        var connector = new InfluxDBSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(InfluxDBConnectorConfig.MeasurementConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithMeasurement()
    {
        var connector = new InfluxDBSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.MeasurementConfig] = "test-measurement"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_SucceedsWithCustomQuery()
    {
        var connector = new InfluxDBSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.QueryConfig] = "from(bucket: \"test\") |> range(start: -1h)"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new InfluxDBSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.MeasurementConfig] = "test-measurement"
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
        var connector = new InfluxDBSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InfluxDBConnectorConfig.UrlConfig] = "http://localhost:8086",
            [InfluxDBConnectorConfig.TokenConfig] = "test-token",
            [InfluxDBConnectorConfig.OrgConfig] = "test-org",
            [InfluxDBConnectorConfig.BucketConfig] = "test-bucket",
            [InfluxDBConnectorConfig.MeasurementConfig] = "test-measurement"
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
        var connector = new InfluxDBSourceConnector();
        var tokenKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.TokenConfig);
        Assert.Equal(ConfigType.Password, tokenKey.Type);
    }

    [Fact]
    public void Config_UrlIsHighImportance()
    {
        var connector = new InfluxDBSourceConnector();
        var urlKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.UrlConfig);
        Assert.Equal(Importance.High, urlKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedTopicPatternDefault()
    {
        var connector = new InfluxDBSourceConnector();
        var topicPatternKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.TopicPatternConfig);
        Assert.Equal(InfluxDBConnectorConfig.DefaultTopicPattern, topicPatternKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedTimeRangeDefault()
    {
        var connector = new InfluxDBSourceConnector();
        var timeRangeKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.TimeRangeConfig);
        Assert.Equal(InfluxDBConnectorConfig.DefaultTimeRange, timeRangeKey.DefaultValue);
    }

    [Fact]
    public void Config_IncludeMetadataDefaultsToTrue()
    {
        var connector = new InfluxDBSourceConnector();
        var includeMetadataKey = connector.Config.Keys.First(k => k.Name == InfluxDBConnectorConfig.IncludeMetadataConfig);
        Assert.Equal(true, includeMetadataKey.DefaultValue);
    }
}

public class InfluxDBSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new InfluxDBSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PollAsync_ReturnsEmptyWhenNotStarted()
    {
        using var task = new InfluxDBSourceTask();
        var result = await task.PollAsync(CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public async Task CommitAsync_CompletesSuccessfully()
    {
        using var task = new InfluxDBSourceTask();
        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void CommitRecord_CompletesSuccessfully()
    {
        using var task = new InfluxDBSourceTask();
        var record = new SourceRecord
        {
            Topic = "test",
            Value = [],
            SourcePartition = new Dictionary<string, object>(),
            SourceOffset = new Dictionary<string, object>()
        };
        var metadata = new RecordMetadata
        {
            Topic = "test",
            Partition = 0,
            Offset = 0
        };

        var exception = Record.Exception(() => task.CommitRecord(record, metadata));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new InfluxDBSourceTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new InfluxDBSourceTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }
}
