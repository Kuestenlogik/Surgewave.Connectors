using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Matter;

/// <summary>
/// Source connector that monitors Matter devices via a Matter controller.
/// </summary>
[ConnectorMetadata(
    Name = "matter-source",
    Description = "Monitors Matter smart home devices via Matter controller API",
    Author = "Surgewave",
    Tags = "matter, iot, smart-home, source")]
public sealed class MatterSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(MatterConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce Matter device events to", EditorHint.Topic)
        .Define(MatterConnectorConfig.ControllerUrl, ConfigType.String, Importance.High,
            "Matter controller API URL (e.g., http://localhost:5580)")
        .Define(MatterConnectorConfig.ApiKey, ConfigType.Password, "", Importance.Medium,
            "Matter controller API key (if required)")
        .Define(MatterConnectorConfig.PollIntervalMs, ConfigType.Int,
            MatterConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(MatterConnectorConfig.IncludeLighting, ConfigType.Boolean, "true", Importance.Medium,
            "Include lighting devices")
        .Define(MatterConnectorConfig.IncludeSensors, ConfigType.Boolean, "true", Importance.Medium,
            "Include sensor devices")
        .Define(MatterConnectorConfig.IncludeSwitches, ConfigType.Boolean, "true", Importance.Medium,
            "Include switch devices")
        .Define(MatterConnectorConfig.IncludeThermostat, ConfigType.Boolean, "true", Importance.Medium,
            "Include thermostat devices")
        .Define(MatterConnectorConfig.IncludeDoorLock, ConfigType.Boolean, "true", Importance.Medium,
            "Include door lock devices")
        .Define(MatterConnectorConfig.EventsOnly, ConfigType.Boolean, "true", Importance.Medium,
            "Only emit on state changes")
        .Define(MatterConnectorConfig.FilterNodeIds, ConfigType.List, "", Importance.Low,
            "Filter by node IDs (comma-separated, empty = all)");

    public override Type TaskClass => typeof(MatterSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(MatterConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{MatterConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(MatterConnectorConfig.ControllerUrl, out var url) ||
            string.IsNullOrWhiteSpace(url))
        {
            throw new ArgumentException($"'{MatterConnectorConfig.ControllerUrl}' is required");
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
