using Kuestenlogik.Surgewave.Connector.ICal;

namespace Kuestenlogik.Surgewave.Connector.ICal.Tests;

public class ICalConnectorConfigTests
{
    [Fact]
    public void TopicConfig_HasExpectedValue()
    {
        Assert.Equal("topic", ICalConnectorConfig.TopicConfig);
    }

    [Fact]
    public void TopicsConfig_HasExpectedValue()
    {
        Assert.Equal("topics", ICalConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void SourceModeConfig_HasExpectedValue()
    {
        Assert.Equal("ical.source.mode", ICalConnectorConfig.SourceModeConfig);
    }

    [Fact]
    public void UrlConfig_HasExpectedValue()
    {
        Assert.Equal("ical.url", ICalConnectorConfig.UrlConfig);
    }

    [Fact]
    public void FilePathConfig_HasExpectedValue()
    {
        Assert.Equal("ical.file.path", ICalConnectorConfig.FilePathConfig);
    }

    [Fact]
    public void PollIntervalMsConfig_HasExpectedValue()
    {
        Assert.Equal("ical.poll.interval.ms", ICalConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void IncludePastEventsConfig_HasExpectedValue()
    {
        Assert.Equal("ical.include.past.events", ICalConnectorConfig.IncludePastEventsConfig);
    }

    [Fact]
    public void TimeWindowDaysConfig_HasExpectedValue()
    {
        Assert.Equal("ical.time.window.days", ICalConnectorConfig.TimeWindowDaysConfig);
    }

    [Fact]
    public void AuthHeaderConfig_HasExpectedValue()
    {
        Assert.Equal("ical.auth.header", ICalConnectorConfig.AuthHeaderConfig);
    }

    [Fact]
    public void AuthTokenConfig_HasExpectedValue()
    {
        Assert.Equal("ical.auth.token", ICalConnectorConfig.AuthTokenConfig);
    }

    [Fact]
    public void HeadersConfig_HasExpectedValue()
    {
        Assert.Equal("ical.headers", ICalConnectorConfig.HeadersConfig);
    }

    [Fact]
    public void TimeoutMsConfig_HasExpectedValue()
    {
        Assert.Equal("ical.timeout.ms", ICalConnectorConfig.TimeoutMsConfig);
    }

    [Fact]
    public void OutputModeConfig_HasExpectedValue()
    {
        Assert.Equal("ical.output.mode", ICalConnectorConfig.OutputModeConfig);
    }

    [Fact]
    public void OutputPathConfig_HasExpectedValue()
    {
        Assert.Equal("ical.output.path", ICalConnectorConfig.OutputPathConfig);
    }

    [Fact]
    public void CalendarNameConfig_HasExpectedValue()
    {
        Assert.Equal("ical.calendar.name", ICalConnectorConfig.CalendarNameConfig);
    }

    [Fact]
    public void CalendarProductIdConfig_HasExpectedValue()
    {
        Assert.Equal("ical.calendar.prodid", ICalConnectorConfig.CalendarProductIdConfig);
    }

    [Fact]
    public void DefaultDurationMinutesConfig_HasExpectedValue()
    {
        Assert.Equal("ical.default.duration.minutes", ICalConnectorConfig.DefaultDurationMinutesConfig);
    }

    [Fact]
    public void SummaryFieldConfig_HasExpectedValue()
    {
        Assert.Equal("ical.summary.field", ICalConnectorConfig.SummaryFieldConfig);
    }

    [Fact]
    public void DescriptionFieldConfig_HasExpectedValue()
    {
        Assert.Equal("ical.description.field", ICalConnectorConfig.DescriptionFieldConfig);
    }

    [Fact]
    public void StartFieldConfig_HasExpectedValue()
    {
        Assert.Equal("ical.start.field", ICalConnectorConfig.StartFieldConfig);
    }

    [Fact]
    public void EndFieldConfig_HasExpectedValue()
    {
        Assert.Equal("ical.end.field", ICalConnectorConfig.EndFieldConfig);
    }

    [Fact]
    public void LocationFieldConfig_HasExpectedValue()
    {
        Assert.Equal("ical.location.field", ICalConnectorConfig.LocationFieldConfig);
    }

    [Fact]
    public void UidFieldConfig_HasExpectedValue()
    {
        Assert.Equal("ical.uid.field", ICalConnectorConfig.UidFieldConfig);
    }

    [Fact]
    public void FlushIntervalMsConfig_HasExpectedValue()
    {
        Assert.Equal("ical.flush.interval.ms", ICalConnectorConfig.FlushIntervalMsConfig);
    }

    [Fact]
    public void MaxEventsPerFileConfig_HasExpectedValue()
    {
        Assert.Equal("ical.max.events.per.file", ICalConnectorConfig.MaxEventsPerFileConfig);
    }

    [Fact]
    public void SourceModeUrl_HasExpectedValue()
    {
        Assert.Equal("url", ICalConnectorConfig.SourceModeUrl);
    }

    [Fact]
    public void SourceModeFile_HasExpectedValue()
    {
        Assert.Equal("file", ICalConnectorConfig.SourceModeFile);
    }

    [Fact]
    public void OutputModeFile_HasExpectedValue()
    {
        Assert.Equal("file", ICalConnectorConfig.OutputModeFile);
    }

    [Fact]
    public void OutputModeRecord_HasExpectedValue()
    {
        Assert.Equal("record", ICalConnectorConfig.OutputModeRecord);
    }

    [Fact]
    public void DefaultSourceMode_HasExpectedValue()
    {
        Assert.Equal("url", ICalConnectorConfig.DefaultSourceMode);
    }

    [Fact]
    public void DefaultOutputMode_HasExpectedValue()
    {
        Assert.Equal("record", ICalConnectorConfig.DefaultOutputMode);
    }

    [Fact]
    public void DefaultPollIntervalMs_HasExpectedValue()
    {
        Assert.Equal(60000, ICalConnectorConfig.DefaultPollIntervalMs);
    }

    [Fact]
    public void DefaultTimeWindowDays_HasExpectedValue()
    {
        Assert.Equal(30, ICalConnectorConfig.DefaultTimeWindowDays);
    }

    [Fact]
    public void DefaultTimeoutMs_HasExpectedValue()
    {
        Assert.Equal(30000, ICalConnectorConfig.DefaultTimeoutMs);
    }

    [Fact]
    public void DefaultDurationMinutes_HasExpectedValue()
    {
        Assert.Equal(60, ICalConnectorConfig.DefaultDurationMinutes);
    }

    [Fact]
    public void DefaultFlushIntervalMs_HasExpectedValue()
    {
        Assert.Equal(10000, ICalConnectorConfig.DefaultFlushIntervalMs);
    }

    [Fact]
    public void DefaultMaxEventsPerFile_HasExpectedValue()
    {
        Assert.Equal(100, ICalConnectorConfig.DefaultMaxEventsPerFile);
    }

    [Fact]
    public void DefaultCalendarName_HasExpectedValue()
    {
        Assert.Equal("Surgewave Calendar", ICalConnectorConfig.DefaultCalendarName);
    }

    [Fact]
    public void DefaultCalendarProductId_HasExpectedValue()
    {
        Assert.Equal("-//Surgewave//Kuestenlogik.Surgewave.Connect.ICal//EN", ICalConnectorConfig.DefaultCalendarProductId);
    }

    [Fact]
    public void DefaultAuthHeader_HasExpectedValue()
    {
        Assert.Equal("Authorization", ICalConnectorConfig.DefaultAuthHeader);
    }

    [Fact]
    public void OffsetLastModified_HasExpectedValue()
    {
        Assert.Equal("last_modified", ICalConnectorConfig.OffsetLastModified);
    }

    [Fact]
    public void OffsetLastPoll_HasExpectedValue()
    {
        Assert.Equal("last_poll", ICalConnectorConfig.OffsetLastPoll);
    }

    [Fact]
    public void OffsetLastEventUid_HasExpectedValue()
    {
        Assert.Equal("last_event_uid", ICalConnectorConfig.OffsetLastEventUid);
    }
}
