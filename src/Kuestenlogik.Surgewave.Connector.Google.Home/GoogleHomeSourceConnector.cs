using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Google.Home;

/// <summary>
/// Source connector that monitors Google Home smart home devices.
/// </summary>
[ConnectorMetadata(
    Name = "google-home-source",
    Description = "Monitors Google Home smart home devices via Home Graph API",
    Author = "Surgewave",
    Tags = "google, home, iot, smart-home, assistant, source")]
public sealed class GoogleHomeSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(GoogleHomeConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce Google Home device events to", EditorHint.Topic)
        .Define(GoogleHomeConnectorConfig.ServiceAccountJson, ConfigType.Password, "", Importance.High,
            "Google service account JSON credentials (inline)")
        .Define(GoogleHomeConnectorConfig.ServiceAccountFile, ConfigType.String, "", Importance.High,
            "Path to Google service account JSON file")
        .Define(GoogleHomeConnectorConfig.AgentUserId, ConfigType.String, Importance.High,
            "Agent user ID for the smart home action")
        .Define(GoogleHomeConnectorConfig.PollIntervalMs, ConfigType.Int,
            GoogleHomeConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(GoogleHomeConnectorConfig.IncludeLights, ConfigType.Boolean, "true", Importance.Medium,
            "Include lighting devices")
        .Define(GoogleHomeConnectorConfig.IncludeSwitches, ConfigType.Boolean, "true", Importance.Medium,
            "Include switch devices")
        .Define(GoogleHomeConnectorConfig.IncludeThermostats, ConfigType.Boolean, "true", Importance.Medium,
            "Include thermostat devices")
        .Define(GoogleHomeConnectorConfig.IncludeLocks, ConfigType.Boolean, "true", Importance.Medium,
            "Include lock devices")
        .Define(GoogleHomeConnectorConfig.IncludeSensors, ConfigType.Boolean, "true", Importance.Medium,
            "Include sensor devices")
        .Define(GoogleHomeConnectorConfig.EventsOnly, ConfigType.Boolean, "true", Importance.Medium,
            "Only emit on state changes")
        .Define(GoogleHomeConnectorConfig.FilterDeviceIds, ConfigType.List, "", Importance.Low,
            "Filter by device IDs (comma-separated, empty = all)");

    public override Type TaskClass => typeof(GoogleHomeSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(GoogleHomeConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{GoogleHomeConnectorConfig.Topic}' is required");
        }

        var hasJson = config.TryGetValue(GoogleHomeConnectorConfig.ServiceAccountJson, out var json) &&
                      !string.IsNullOrWhiteSpace(json);
        var hasFile = config.TryGetValue(GoogleHomeConnectorConfig.ServiceAccountFile, out var file) &&
                      !string.IsNullOrWhiteSpace(file);

        if (!hasJson && !hasFile)
        {
            throw new ArgumentException($"Either '{GoogleHomeConnectorConfig.ServiceAccountJson}' or '{GoogleHomeConnectorConfig.ServiceAccountFile}' is required");
        }

        if (!config.TryGetValue(GoogleHomeConnectorConfig.AgentUserId, out var userId) ||
            string.IsNullOrWhiteSpace(userId))
        {
            throw new ArgumentException($"'{GoogleHomeConnectorConfig.AgentUserId}' is required");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
