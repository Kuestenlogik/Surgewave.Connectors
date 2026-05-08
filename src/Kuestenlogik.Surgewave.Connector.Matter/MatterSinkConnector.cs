using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Matter;

/// <summary>
/// Sink connector that controls Matter devices via a Matter controller.
/// </summary>
[ConnectorMetadata(
    Name = "matter-sink",
    Description = "Controls Matter smart home devices via Matter controller API",
    Author = "Surgewave",
    Tags = "matter, iot, smart-home, sink")]
public sealed class MatterSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(MatterConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume device commands from", EditorHint.Topic)
        .Define(MatterConnectorConfig.ControllerUrl, ConfigType.String, Importance.High,
            "Matter controller API URL (e.g., http://localhost:5580)")
        .Define(MatterConnectorConfig.ApiKey, ConfigType.Password, "", Importance.Medium,
            "Matter controller API key (if required)")
        .Define(MatterConnectorConfig.DefaultNodeId, ConfigType.String, "", Importance.Medium,
            "Default node ID when not specified in message")
        .Define(MatterConnectorConfig.DefaultEndpointId, ConfigType.Int,
            MatterConnectorConfig.DefaultEndpointIdValue.ToString(), Importance.Low,
            "Default endpoint ID (typically 1)");

    public override Type TaskClass => typeof(MatterSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(MatterConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{MatterConnectorConfig.Topics}' is required");
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
