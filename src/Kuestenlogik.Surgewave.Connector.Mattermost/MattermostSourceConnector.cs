using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Mattermost;

/// <summary>
/// Source connector for receiving messages from Mattermost.
/// </summary>
[ConnectorMetadata(
    Name = "mattermost-source",
    Description = "Receives messages from Mattermost channels via REST API",
    Author = "Surgewave",
    Tags = "mattermost,chat,messaging,source")]
public sealed class MattermostSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(MattermostSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(MattermostConnectorConfig.ServerUrl, ConfigType.String, MattermostConnectorConfig.DefaultServerUrl, Importance.High, "Mattermost server URL")
        .Define(MattermostConnectorConfig.AccessToken, ConfigType.Password, "", Importance.High, "Personal access token")
        .Define(MattermostConnectorConfig.Topic, ConfigType.String, Importance.High, "Surgewave topic to write messages to", EditorHint.Topic)
        .Define(MattermostConnectorConfig.ChannelIds, ConfigType.String, "", Importance.Medium, "Comma-separated list of channel IDs to monitor (empty for all)")
        .Define(MattermostConnectorConfig.IncludeBotMessages, ConfigType.Boolean, MattermostConnectorConfig.DefaultIncludeBotMessages, Importance.Low, "Include messages from bots")
        .Define(MattermostConnectorConfig.PollIntervalMs, ConfigType.Int, MattermostConnectorConfig.DefaultPollIntervalMs, Importance.Medium, "Poll interval in milliseconds");

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
