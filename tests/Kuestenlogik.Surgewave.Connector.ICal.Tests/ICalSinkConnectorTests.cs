using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.ICal;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.ICal.Tests;

public class ICalSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new ICalSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsICalSinkTask()
    {
        var connector = new ICalSinkConnector();
        Assert.Equal(typeof(ICalSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesOutputModeConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.OutputModeConfig);
    }

    [Fact]
    public void Config_DefinesOutputPathConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.OutputPathConfig);
    }

    [Fact]
    public void Config_DefinesCalendarNameConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.CalendarNameConfig);
    }

    [Fact]
    public void Config_DefinesCalendarProductIdConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.CalendarProductIdConfig);
    }

    [Fact]
    public void Config_DefinesDefaultDurationMinutesConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.DefaultDurationMinutesConfig);
    }

    [Fact]
    public void Config_DefinesSummaryFieldConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.SummaryFieldConfig);
    }

    [Fact]
    public void Config_DefinesDescriptionFieldConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.DescriptionFieldConfig);
    }

    [Fact]
    public void Config_DefinesStartFieldConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.StartFieldConfig);
    }

    [Fact]
    public void Config_DefinesEndFieldConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.EndFieldConfig);
    }

    [Fact]
    public void Config_DefinesLocationFieldConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.LocationFieldConfig);
    }

    [Fact]
    public void Config_DefinesUidFieldConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.UidFieldConfig);
    }

    [Fact]
    public void Config_DefinesFlushIntervalMsConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.FlushIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesMaxEventsPerFileConfig()
    {
        var connector = new ICalSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ICalConnectorConfig.MaxEventsPerFileConfig);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new ICalSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.OutputModeConfig] = ICalConnectorConfig.OutputModeRecord
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ICalConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenFileModeWithoutOutputPath()
    {
        var connector = new ICalSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicsConfig] = "events",
            [ICalConnectorConfig.OutputModeConfig] = ICalConnectorConfig.OutputModeFile
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ICalConnectorConfig.OutputPathConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithRecordMode()
    {
        var connector = new ICalSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicsConfig] = "events"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_SucceedsWithFileMode()
    {
        var connector = new ICalSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicsConfig] = "events",
            [ICalConnectorConfig.OutputModeConfig] = ICalConnectorConfig.OutputModeFile,
            [ICalConnectorConfig.OutputPathConfig] = "/tmp/events.ics"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new ICalSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicsConfig] = "events"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[ICalConnectorConfig.TopicsConfig], taskConfigs[0][ICalConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new ICalSinkConnector();
        var config = new Dictionary<string, string>
        {
            [ICalConnectorConfig.TopicsConfig] = "events"
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
    public void Config_TopicsIsHighImportance()
    {
        var connector = new ICalSinkConnector();
        var topicsKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, topicsKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedOutputModeDefault()
    {
        var connector = new ICalSinkConnector();
        var outputModeKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.OutputModeConfig);
        Assert.Equal(ICalConnectorConfig.DefaultOutputMode, outputModeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedCalendarNameDefault()
    {
        var connector = new ICalSinkConnector();
        var calNameKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.CalendarNameConfig);
        Assert.Equal(ICalConnectorConfig.DefaultCalendarName, calNameKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedDurationDefault()
    {
        var connector = new ICalSinkConnector();
        var durationKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.DefaultDurationMinutesConfig);
        Assert.Equal(ICalConnectorConfig.DefaultDurationMinutes, durationKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxEventsDefault()
    {
        var connector = new ICalSinkConnector();
        var maxEventsKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.MaxEventsPerFileConfig);
        Assert.Equal(ICalConnectorConfig.DefaultMaxEventsPerFile, maxEventsKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedFlushIntervalDefault()
    {
        var connector = new ICalSinkConnector();
        var flushKey = connector.Config.Keys.First(k => k.Name == ICalConnectorConfig.FlushIntervalMsConfig);
        Assert.Equal(ICalConnectorConfig.DefaultFlushIntervalMs, flushKey.DefaultValue);
    }
}

public class ICalSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new ICalSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PutAsync_SkipsNullValues()
    {
        using var task = new ICalSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = null! }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_SkipsEmptyValues()
    {
        using var task = new ICalSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = [] }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_HandlesInvalidJson()
    {
        using var task = new ICalSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = System.Text.Encoding.UTF8.GetBytes("not valid json") }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_CompletesSuccessfully()
    {
        using var task = new ICalSinkTask();
        var offsets = new Dictionary<TopicPartition, long>();

        var exception = await Record.ExceptionAsync(() => task.FlushAsync(offsets, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new ICalSinkTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new ICalSinkTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_HandlesEmptyRecordsList()
    {
        using var task = new ICalSinkTask();
        var records = new List<SinkRecord>();

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }
}
