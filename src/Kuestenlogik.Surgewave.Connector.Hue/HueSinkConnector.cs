using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Hue;

/// <summary>
/// Sink connector that controls Philips Hue lights and groups.
/// </summary>
[ConnectorMetadata(
    Name = "hue-sink",
    Description = "Controls Philips Hue lights and groups via bridge API",
    Author = "Surgewave",
    Tags = "hue, philips, iot, smart-home, lighting, sink")]
public sealed class HueSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(HueConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume control commands from", EditorHint.Topic)
        .Define(HueConnectorConfig.BridgeIp, ConfigType.String, Importance.High,
            "Philips Hue bridge IP address")
        .Define(HueConnectorConfig.AppKey, ConfigType.Password, Importance.High,
            "Hue bridge application key (username)")
        .Define(HueConnectorConfig.DefaultLightId, ConfigType.String, "", Importance.Medium,
            "Default light ID when not specified in message")
        .Define(HueConnectorConfig.DefaultGroupId, ConfigType.String, "", Importance.Medium,
            "Default group ID when not specified in message")
        .Define(HueConnectorConfig.TransitionTimeMs, ConfigType.Int,
            HueConnectorConfig.DefaultTransitionTimeMs.ToString(), Importance.Low,
            "Default transition time in milliseconds");

    public override Type TaskClass => typeof(HueSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(HueConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{HueConnectorConfig.Topics}' is required");
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
