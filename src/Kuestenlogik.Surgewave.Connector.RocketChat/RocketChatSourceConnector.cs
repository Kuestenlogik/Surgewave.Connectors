using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.RocketChat;

/// <summary>
/// Source connector that receives messages from Rocket.Chat rooms via REST API.
/// </summary>
[ConnectorMetadata(
    Name = "rocketchat-source",
    Description = "Receives messages from Rocket.Chat rooms via REST API polling",
    Author = "Surgewave",
    Tags = "rocketchat,chat,messaging,source")]
public sealed class RocketChatSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(RocketChatSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(RocketChatConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Destination topic for Rocket.Chat messages", EditorHint.Topic)
        .Define(RocketChatConnectorConfig.ServerUrl, ConfigType.String, RocketChatConnectorConfig.DefaultServerUrl, Importance.High,
            "Rocket.Chat server URL")
        .Define(RocketChatConnectorConfig.UserId, ConfigType.String, Importance.High,
            "Rocket.Chat user ID for authentication")
        .Define(RocketChatConnectorConfig.AuthToken, ConfigType.Password, Importance.High,
            "Rocket.Chat auth token")
        .Define(RocketChatConnectorConfig.RoomIds, ConfigType.String, Importance.High,
            "Comma-separated list of room IDs to monitor")
        .Define(RocketChatConnectorConfig.PollIntervalMs, ConfigType.Int, RocketChatConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(RocketChatConnectorConfig.IncludeBotMessages, ConfigType.Boolean, RocketChatConnectorConfig.DefaultIncludeBotMessages, Importance.Low,
            "Include messages from bots");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
