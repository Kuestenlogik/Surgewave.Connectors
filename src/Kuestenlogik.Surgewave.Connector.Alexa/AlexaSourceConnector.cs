using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Alexa;

/// <summary>
/// Source connector that monitors Alexa smart home devices.
/// </summary>
[ConnectorMetadata(
    Name = "alexa-source",
    Description = "Monitors Amazon Alexa smart home devices via Alexa API",
    Author = "Surgewave",
    Tags = "alexa, amazon, iot, smart-home, voice, source")]
public sealed class AlexaSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(AlexaConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce Alexa device events to", EditorHint.Topic)
        .Define(AlexaConnectorConfig.ClientId, ConfigType.String, Importance.High,
            "Alexa skill client ID (from Alexa Developer Console)")
        .Define(AlexaConnectorConfig.ClientSecret, ConfigType.Password, Importance.High,
            "Alexa skill client secret")
        .Define(AlexaConnectorConfig.RefreshToken, ConfigType.Password, Importance.High,
            "Alexa API refresh token")
        .Define(AlexaConnectorConfig.Region, ConfigType.String,
            AlexaConnectorConfig.DefaultRegion, Importance.Medium,
            "Alexa region (NA, EU, FE)", EditorHint.Select, options: ["NA", "EU", "FE"])
        .Define(AlexaConnectorConfig.PollIntervalMs, ConfigType.Int,
            AlexaConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(AlexaConnectorConfig.IncludeLights, ConfigType.Boolean, "true", Importance.Medium,
            "Include lighting devices")
        .Define(AlexaConnectorConfig.IncludeSwitches, ConfigType.Boolean, "true", Importance.Medium,
            "Include switch devices")
        .Define(AlexaConnectorConfig.IncludeThermostats, ConfigType.Boolean, "true", Importance.Medium,
            "Include thermostat devices")
        .Define(AlexaConnectorConfig.IncludeLocks, ConfigType.Boolean, "true", Importance.Medium,
            "Include lock devices")
        .Define(AlexaConnectorConfig.IncludeSensors, ConfigType.Boolean, "true", Importance.Medium,
            "Include sensor devices")
        .Define(AlexaConnectorConfig.EventsOnly, ConfigType.Boolean, "true", Importance.Medium,
            "Only emit on state changes")
        .Define(AlexaConnectorConfig.FilterEndpointIds, ConfigType.List, "", Importance.Low,
            "Filter by endpoint IDs (comma-separated, empty = all)");

    public override Type TaskClass => typeof(AlexaSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(AlexaConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{AlexaConnectorConfig.Topic}' is required");
        }

        if (!config.TryGetValue(AlexaConnectorConfig.ClientId, out var clientId) ||
            string.IsNullOrWhiteSpace(clientId))
        {
            throw new ArgumentException($"'{AlexaConnectorConfig.ClientId}' is required");
        }

        if (!config.TryGetValue(AlexaConnectorConfig.ClientSecret, out var clientSecret) ||
            string.IsNullOrWhiteSpace(clientSecret))
        {
            throw new ArgumentException($"'{AlexaConnectorConfig.ClientSecret}' is required");
        }

        if (!config.TryGetValue(AlexaConnectorConfig.RefreshToken, out var refreshToken) ||
            string.IsNullOrWhiteSpace(refreshToken))
        {
            throw new ArgumentException($"'{AlexaConnectorConfig.RefreshToken}' is required");
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
