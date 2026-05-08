using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.ICal;

/// <summary>
/// Sink connector that generates iCal/ICS calendar files from records.
/// </summary>
public sealed class ICalSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ICalSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        .Define(ICalConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(ICalConnectorConfig.OutputModeConfig, ConfigType.String, ICalConnectorConfig.DefaultOutputMode, Importance.Medium,
            "Output mode: 'file' (write to .ics file) or 'record' (emit as record value)")
        .Define(ICalConnectorConfig.OutputPathConfig, ConfigType.String, "", Importance.Medium,
            "Output file path for file mode (supports ${topic} and ${timestamp} placeholders)", EditorHint.FilePath)
        .Define(ICalConnectorConfig.CalendarNameConfig, ConfigType.String, ICalConnectorConfig.DefaultCalendarName, Importance.Low,
            "Calendar name (X-WR-CALNAME)")
        .Define(ICalConnectorConfig.CalendarProductIdConfig, ConfigType.String, ICalConnectorConfig.DefaultCalendarProductId, Importance.Low,
            "Calendar product ID (PRODID)")
        .Define(ICalConnectorConfig.DefaultDurationMinutesConfig, ConfigType.Int, ICalConnectorConfig.DefaultDurationMinutes, Importance.Low,
            "Default event duration in minutes when end time not specified")
        .Define(ICalConnectorConfig.SummaryFieldConfig, ConfigType.String, "summary", Importance.Low,
            "JSON field for event summary/title")
        .Define(ICalConnectorConfig.DescriptionFieldConfig, ConfigType.String, "description", Importance.Low,
            "JSON field for event description")
        .Define(ICalConnectorConfig.StartFieldConfig, ConfigType.String, "start", Importance.Low,
            "JSON field for event start time (ISO 8601)")
        .Define(ICalConnectorConfig.EndFieldConfig, ConfigType.String, "end", Importance.Low,
            "JSON field for event end time (ISO 8601)")
        .Define(ICalConnectorConfig.LocationFieldConfig, ConfigType.String, "location", Importance.Low,
            "JSON field for event location")
        .Define(ICalConnectorConfig.UidFieldConfig, ConfigType.String, "uid", Importance.Low,
            "JSON field for event UID (auto-generated if not present)")
        .Define(ICalConnectorConfig.FlushIntervalMsConfig, ConfigType.Int, ICalConnectorConfig.DefaultFlushIntervalMs, Importance.Low,
            "Flush interval in milliseconds for file mode")
        .Define(ICalConnectorConfig.MaxEventsPerFileConfig, ConfigType.Int, ICalConnectorConfig.DefaultMaxEventsPerFile, Importance.Low,
            "Maximum events per file before rotation");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(ICalConnectorConfig.TopicsConfig, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"Missing required config: {ICalConnectorConfig.TopicsConfig}");
        }

        var mode = config.TryGetValue(ICalConnectorConfig.OutputModeConfig, out var m)
            ? m : ICalConnectorConfig.DefaultOutputMode;

        if (mode == ICalConnectorConfig.OutputModeFile)
        {
            if (!config.TryGetValue(ICalConnectorConfig.OutputPathConfig, out var path) ||
                string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentException($"File mode requires {ICalConnectorConfig.OutputPathConfig}");
            }
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
