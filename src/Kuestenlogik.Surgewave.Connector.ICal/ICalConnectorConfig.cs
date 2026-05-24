namespace Kuestenlogik.Surgewave.Connector.ICal;

/// <summary>
/// Configuration constants for iCal connectors.
/// </summary>
public static class ICalConnectorConfig
{
    // Common settings
    public const string TopicConfig = "topic";
    public const string TopicsConfig = "topics";

    // Source settings
    public const string SourceModeConfig = "ical.source.mode";
    public const string UrlConfig = "ical.url";
    public const string FilePathConfig = "ical.file.path";
    public const string PollIntervalMsConfig = "ical.poll.interval.ms";
    public const string IncludePastEventsConfig = "ical.include.past.events";
    public const string TimeWindowDaysConfig = "ical.time.window.days";
    public const string AuthHeaderConfig = "ical.auth.header";
    public const string AuthTokenConfig = "ical.auth.token";
    public const string HeadersConfig = "ical.headers";
    public const string TimeoutMsConfig = "ical.timeout.ms";

    // Sink settings
    public const string OutputModeConfig = "ical.output.mode";
    public const string OutputPathConfig = "ical.output.path";
    public const string CalendarNameConfig = "ical.calendar.name";
    public const string CalendarProductIdConfig = "ical.calendar.prodid";
    public const string DefaultDurationMinutesConfig = "ical.default.duration.minutes";
    public const string SummaryFieldConfig = "ical.summary.field";
    public const string DescriptionFieldConfig = "ical.description.field";
    public const string StartFieldConfig = "ical.start.field";
    public const string EndFieldConfig = "ical.end.field";
    public const string LocationFieldConfig = "ical.location.field";
    public const string UidFieldConfig = "ical.uid.field";
    public const string FlushIntervalMsConfig = "ical.flush.interval.ms";
    public const string MaxEventsPerFileConfig = "ical.max.events.per.file";

    // Source modes
    public const string SourceModeUrl = "url";
    public const string SourceModeFile = "file";

    // Output modes
    public const string OutputModeFile = "file";
    public const string OutputModeRecord = "record";

    // Defaults
    public const string DefaultSourceMode = SourceModeUrl;
    public const string DefaultOutputMode = OutputModeRecord;
    public const int DefaultPollIntervalMs = 60000;
    public const int DefaultTimeWindowDays = 30;
    public const int DefaultTimeoutMs = 30000;
    public const int DefaultDurationMinutes = 60;
    public const int DefaultFlushIntervalMs = 10000;
    public const int DefaultMaxEventsPerFile = 100;
    public const string DefaultCalendarName = "Surgewave Calendar";
    public const string DefaultCalendarProductId = "-//Surgewave//Kuestenlogik.Surgewave.Connect.ICal//EN";
    public const string DefaultAuthHeader = "Authorization";

    // Offset keys
    public const string OffsetLastModified = "last_modified";
    public const string OffsetLastPoll = "last_poll";
    public const string OffsetLastEventUid = "last_event_uid";
}
