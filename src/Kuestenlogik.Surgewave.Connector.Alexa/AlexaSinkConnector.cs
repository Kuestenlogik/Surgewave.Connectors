using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Alexa;

/// <summary>
/// Sink connector that controls Alexa smart home devices.
/// </summary>
[ConnectorMetadata(
    Name = "alexa-sink",
    Description = "Controls Amazon Alexa smart home devices via Alexa API",
    Author = "Surgewave",
    Tags = "alexa, amazon, iot, smart-home, voice, sink")]
public sealed class AlexaSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(AlexaConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume device commands from", EditorHint.Topic)
        .Define(AlexaConnectorConfig.ClientId, ConfigType.String, Importance.High,
            "Alexa skill client ID")
        .Define(AlexaConnectorConfig.ClientSecret, ConfigType.Password, Importance.High,
            "Alexa skill client secret")
        .Define(AlexaConnectorConfig.RefreshToken, ConfigType.Password, Importance.High,
            "Alexa API refresh token")
        .Define(AlexaConnectorConfig.Region, ConfigType.String,
            AlexaConnectorConfig.DefaultRegion, Importance.Medium,
            "Alexa region (NA, EU, FE)", EditorHint.Select, options: ["NA", "EU", "FE"])
        .Define(AlexaConnectorConfig.DefaultEndpointId, ConfigType.String, "", Importance.Medium,
            "Default endpoint ID when not specified in message");

    public override Type TaskClass => typeof(AlexaSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(AlexaConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{AlexaConnectorConfig.Topics}' is required");
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
