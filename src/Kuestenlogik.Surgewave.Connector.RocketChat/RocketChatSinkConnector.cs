using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.RocketChat;

/// <summary>
/// Sink connector that posts messages to Rocket.Chat rooms via REST API.
/// </summary>
[ConnectorMetadata(
    Name = "rocketchat-sink",
    Description = "Posts messages to Rocket.Chat rooms via REST API",
    Author = "Surgewave",
    Tags = "rocketchat,chat,messaging,sink")]
public sealed class RocketChatSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(RocketChatSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(RocketChatConnectorConfig.ServerUrl, ConfigType.String, RocketChatConnectorConfig.DefaultServerUrl, Importance.High,
            "Rocket.Chat server URL")
        .Define(RocketChatConnectorConfig.UserId, ConfigType.String, Importance.High,
            "Rocket.Chat user ID for authentication")
        .Define(RocketChatConnectorConfig.AuthToken, ConfigType.Password, Importance.High,
            "Rocket.Chat auth token")
        .Define(RocketChatConnectorConfig.DefaultRoomId, ConfigType.String, Importance.High,
            "Default room ID for messages")
        .Define(RocketChatConnectorConfig.RoomIdField, ConfigType.String, Importance.Low,
            "JSON field containing the room ID")
        .Define(RocketChatConnectorConfig.TextField, ConfigType.String, "text", Importance.Medium,
            "JSON field containing the message text");

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
