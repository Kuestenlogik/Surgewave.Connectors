using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Mattermost;

/// <summary>
/// Sink connector for sending messages to Mattermost.
/// </summary>
[ConnectorMetadata(
    Name = "mattermost-sink",
    Description = "Sends messages to Mattermost channels",
    Author = "Surgewave",
    Tags = "mattermost,chat,messaging,sink")]
public sealed class MattermostSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(MattermostSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(MattermostConnectorConfig.ServerUrl, ConfigType.String, MattermostConnectorConfig.DefaultServerUrl, Importance.High, "Mattermost server URL")
        .Define(MattermostConnectorConfig.AccessToken, ConfigType.Password, Importance.High, "Personal access token")
        .Define(MattermostConnectorConfig.ChannelId, ConfigType.String, Importance.High, "Channel ID to post messages to")
        .Define(MattermostConnectorConfig.MessageField, ConfigType.String, MattermostConnectorConfig.DefaultMessageField, Importance.Medium, "JSON field containing the message text");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
