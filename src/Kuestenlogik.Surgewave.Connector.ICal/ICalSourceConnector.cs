using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.ICal;

/// <summary>
/// Source connector for iCal/ICS calendar files.
/// Polls .ics files or URLs and emits calendar events as records.
/// </summary>
public sealed class ICalSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(ICalSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        .Define(ICalConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination topic for calendar events", EditorHint.Topic)
        .Define(ICalConnectorConfig.SourceModeConfig, ConfigType.String, ICalConnectorConfig.DefaultSourceMode, Importance.Medium,
            "Source mode: 'url' (HTTP/HTTPS) or 'file' (local file)")
        .Define(ICalConnectorConfig.UrlConfig, ConfigType.String, "", Importance.High,
            "URL of the iCal/ICS file (required for url mode)")
        .Define(ICalConnectorConfig.FilePathConfig, ConfigType.String, "", Importance.High,
            "Path to local .ics file (required for file mode)", EditorHint.FilePath)
        .Define(ICalConnectorConfig.PollIntervalMsConfig, ConfigType.Int, ICalConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(ICalConnectorConfig.IncludePastEventsConfig, ConfigType.Boolean, false, Importance.Low,
            "Include events that have already ended")
        .Define(ICalConnectorConfig.TimeWindowDaysConfig, ConfigType.Int, ICalConnectorConfig.DefaultTimeWindowDays, Importance.Low,
            "Number of days to look ahead for events")
        .Define(ICalConnectorConfig.AuthHeaderConfig, ConfigType.String, ICalConnectorConfig.DefaultAuthHeader, Importance.Low,
            "Authentication header name")
        .Define(ICalConnectorConfig.AuthTokenConfig, ConfigType.Password, "", Importance.Medium,
            "Authentication token")
        .Define(ICalConnectorConfig.HeadersConfig, ConfigType.String, "", Importance.Low,
            "Additional HTTP headers as key=value pairs separated by semicolons", EditorHint.Multiline)
        .Define(ICalConnectorConfig.TimeoutMsConfig, ConfigType.Int, ICalConnectorConfig.DefaultTimeoutMs, Importance.Low,
            "HTTP request timeout in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(ICalConnectorConfig.TopicConfig, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"Missing required config: {ICalConnectorConfig.TopicConfig}");
        }

        var mode = config.TryGetValue(ICalConnectorConfig.SourceModeConfig, out var m)
            ? m : ICalConnectorConfig.DefaultSourceMode;

        if (mode == ICalConnectorConfig.SourceModeUrl)
        {
            if (!config.TryGetValue(ICalConnectorConfig.UrlConfig, out var url) ||
                string.IsNullOrWhiteSpace(url))
            {
                throw new ArgumentException($"URL mode requires {ICalConnectorConfig.UrlConfig}");
            }
        }
        else if (mode == ICalConnectorConfig.SourceModeFile)
        {
            if (!config.TryGetValue(ICalConnectorConfig.FilePathConfig, out var path) ||
                string.IsNullOrWhiteSpace(path))
            {
                throw new ArgumentException($"File mode requires {ICalConnectorConfig.FilePathConfig}");
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
