using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Hue;

/// <summary>
/// Source connector that monitors Philips Hue lights, sensors, and groups.
/// </summary>
[ConnectorMetadata(
    Name = "hue-source",
    Description = "Monitors Philips Hue bridge for light, sensor, and group state changes",
    Author = "Surgewave",
    Tags = "hue, philips, iot, smart-home, lighting, source")]
public sealed class HueSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(HueConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce Hue events to", EditorHint.Topic)
        .Define(HueConnectorConfig.BridgeIp, ConfigType.String, Importance.High,
            "Philips Hue bridge IP address")
        .Define(HueConnectorConfig.AppKey, ConfigType.Password, Importance.High,
            "Hue bridge application key (username)")
        .Define(HueConnectorConfig.PollIntervalMs, ConfigType.Int,
            HueConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(HueConnectorConfig.IncludeLights, ConfigType.Boolean, "true", Importance.Medium,
            "Include light states")
        .Define(HueConnectorConfig.IncludeSensors, ConfigType.Boolean, "true", Importance.Medium,
            "Include sensor states")
        .Define(HueConnectorConfig.IncludeGroups, ConfigType.Boolean, "true", Importance.Medium,
            "Include group/room states")
        .Define(HueConnectorConfig.IncludeScenes, ConfigType.Boolean, "false", Importance.Low,
            "Include scene information")
        .Define(HueConnectorConfig.EventsOnly, ConfigType.Boolean, "true", Importance.Medium,
            "Only emit on state changes (not every poll)");

    public override Type TaskClass => typeof(HueSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(HueConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{HueConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(HueConnectorConfig.BridgeIp, out var ip) ||
            string.IsNullOrWhiteSpace(ip))
        {
            throw new ArgumentException($"'{HueConnectorConfig.BridgeIp}' is required");
        }

        if (!config.TryGetValue(HueConnectorConfig.AppKey, out var key) ||
            string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException($"'{HueConnectorConfig.AppKey}' is required");
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
