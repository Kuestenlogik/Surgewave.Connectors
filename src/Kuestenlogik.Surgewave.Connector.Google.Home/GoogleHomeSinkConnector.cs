using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Google.Home;

/// <summary>
/// Sink connector that controls Google Home smart home devices.
/// </summary>
[ConnectorMetadata(
    Name = "google-home-sink",
    Description = "Controls Google Home smart home devices via Home Graph API",
    Author = "Surgewave",
    Tags = "google, home, iot, smart-home, assistant, sink")]
public sealed class GoogleHomeSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(GoogleHomeConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume device commands from", EditorHint.Topic)
        .Define(GoogleHomeConnectorConfig.ServiceAccountJson, ConfigType.Password, "", Importance.High,
            "Google service account JSON credentials (inline)")
        .Define(GoogleHomeConnectorConfig.ServiceAccountFile, ConfigType.String, "", Importance.High,
            "Path to Google service account JSON file")
        .Define(GoogleHomeConnectorConfig.AgentUserId, ConfigType.String, Importance.High,
            "Agent user ID for the smart home action")
        .Define(GoogleHomeConnectorConfig.DefaultDeviceId, ConfigType.String, "", Importance.Medium,
            "Default device ID when not specified in message");

    public override Type TaskClass => typeof(GoogleHomeSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(GoogleHomeConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{GoogleHomeConnectorConfig.Topics}' is required");
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
