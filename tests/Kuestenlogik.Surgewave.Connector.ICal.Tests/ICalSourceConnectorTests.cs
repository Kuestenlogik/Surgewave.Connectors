using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.ICal;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.ICal.Tests;

public class ICalSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new ICalSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsICalSourceTask()
    {
        var connector = new ICalSourceConnector();
        Assert.Equal(typeof(ICalSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesTopicConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.TopicConfig);
    }

    [Fact]
    public void Config_DefinesSourceModeConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.SourceModeConfig);
    }

    [Fact]
    public void Config_DefinesUrlConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.UrlConfig);
    }

    [Fact]
    public void Config_DefinesFilePathConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.FilePathConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalMsConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesIncludePastEventsConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.IncludePastEventsConfig);
    }

    [Fact]
    public void Config_DefinesTimeWindowDaysConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.TimeWindowDaysConfig);
    }

    [Fact]
    public void Config_DefinesAuthHeaderConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.AuthHeaderConfig);
    }

    [Fact]
    public void Config_DefinesAuthTokenConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.AuthTokenConfig);
    }

    [Fact]
    public void Config_DefinesHeadersConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.HeadersConfig);
    }

    [Fact]
    public void Config_DefinesTimeoutMsConfig()
    {
        var connector = new ICalSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.TimeoutMsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenTopicMissing()
    {
        var connector = new ICalSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.UrlConfig] = "https://example.com/calendar.ics"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ICalConnectorConfig.TopicConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenUrlModeWithoutUrl()
    {
        var connector = new ICalSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicConfig] = "calendar-events",
            [ICalConnectorConfig.SourceModeConfig] = ICalConnectorConfig.SourceModeUrl
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ICalConnectorConfig.UrlConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenFileModeWithoutFilePath()
    {
        var connector = new ICalSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicConfig] = "calendar-events",
            [ICalConnectorConfig.SourceModeConfig] = ICalConnectorConfig.SourceModeFile
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ICalConnectorConfig.FilePathConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidUrlConfig()
    {
        var connector = new ICalSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicConfig] = "calendar-events",
            [ICalConnectorConfig.UrlConfig] = "https://example.com/calendar.ics"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_SucceedsWithValidFileConfig()
    {
        var connector = new ICalSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicConfig] = "calendar-events",
            [ICalConnectorConfig.SourceModeConfig] = ICalConnectorConfig.SourceModeFile,
            [ICalConnectorConfig.FilePathConfig] = "/path/to/calendar.ics"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new ICalSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicConfig] = "calendar-events",
            [ICalConnectorConfig.UrlConfig] = "https://example.com/calendar.ics"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[ICalConnectorConfig.UrlConfig], taskConfigs[0][ICalConnectorConfig.UrlConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new ICalSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicConfig] = "calendar-events",
            [ICalConnectorConfig.UrlConfig] = "https://example.com/calendar.ics"
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
    public void Config_AuthTokenIsPasswordType()
    {
        var connector = new ICalSourceConnector();
        var authTokenKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.AuthTokenConfig);
        Assert.Equal(ConfigType.Password, authTokenKey.Type);
    }

    [Fact]
    public void Config_TopicIsHighImportance()
    {
        var connector = new ICalSourceConnector();
        var topicKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.TopicConfig);
        Assert.Equal(Importance.High, topicKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedSourceModeDefault()
    {
        var connector = new ICalSourceConnector();
        var sourceModeKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.SourceModeConfig);
        Assert.Equal(ICalConnectorConfig.DefaultSourceMode, sourceModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new ICalSourceConnector();
        var pollKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.PollIntervalMsConfig);
        Assert.Equal(ICalConnectorConfig.DefaultPollIntervalMs, pollKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedTimeWindowDefault()
    {
        var connector = new ICalSourceConnector();
        var windowKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.TimeWindowDaysConfig);
        Assert.Equal(ICalConnectorConfig.DefaultTimeWindowDays, windowKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedTimeoutDefault()
    {
        var connector = new ICalSourceConnector();
        var timeoutKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.TimeoutMsConfig);
        Assert.Equal(ICalConnectorConfig.DefaultTimeoutMs, timeoutKey.DefaultValue);
    }
}

public class ICalSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new ICalSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PollAsync_ReturnsEmptyWhenNotStarted()
    {
        using var task = new ICalSourceTask();
        var result = await task.PollAsync(CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public async Task CommitAsync_CompletesSuccessfully()
    {
        using var task = new ICalSourceTask();
        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void CommitRecord_CompletesSuccessfully()
    {
        using var task = new ICalSourceTask();
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
        using var task = new ICalSourceTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new ICalSourceTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }
}
