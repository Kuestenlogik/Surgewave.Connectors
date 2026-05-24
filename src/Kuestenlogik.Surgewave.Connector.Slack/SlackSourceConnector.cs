using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Slack;

/// <summary>
/// Source connector that receives events from Slack via Socket Mode or Events API.
/// </summary>
public class SlackSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(SlackSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Topic
        .Define(SlackConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination topic for Slack events", EditorHint.Topic)

        // Slack API
        .Define(SlackConnectorConfig.BotTokenConfig, ConfigType.Password, Importance.High,
            "Slack Bot User OAuth Token (xoxb-...)")
        .Define(SlackConnectorConfig.AppTokenConfig, ConfigType.Password, Importance.High,
            "Slack App-Level Token for Socket Mode (xapp-...)")
        .Define(SlackConnectorConfig.SigningSecretConfig, ConfigType.Password, Importance.Medium,
            "Slack Signing Secret for Events API verification")

        // Source settings
        .Define(SlackConnectorConfig.SocketModeConfig, ConfigType.Boolean, SlackConnectorConfig.DefaultSocketMode, Importance.Medium,
            "Use Socket Mode for real-time events (requires App Token)")
        .Define(SlackConnectorConfig.EventTypesConfig, ConfigType.String, SlackConnectorConfig.DefaultEventTypes, Importance.Medium,
            "Comma-separated list of event types to listen for", EditorHint.Select, options: ["message", "reaction_added", "app_mention", "file_shared"])
        .Define(SlackConnectorConfig.ChannelFilterConfig, ConfigType.String, Importance.Low,
            "Comma-separated list of channel IDs to filter (empty = all)")
        .Define(SlackConnectorConfig.UserFilterConfig, ConfigType.String, Importance.Low,
            "Comma-separated list of user IDs to filter (empty = all)")
        .Define(SlackConnectorConfig.IncludeBotMessagesConfig, ConfigType.Boolean, SlackConnectorConfig.DefaultIncludeBotMessages, Importance.Low,
            "Include messages from bots");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Validate required config
        if (!config.TryGetValue(SlackConnectorConfig.TopicConfig, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"Missing required configuration: {SlackConnectorConfig.TopicConfig}");
        }

        if (!config.TryGetValue(SlackConnectorConfig.BotTokenConfig, out var botToken) ||
            string.IsNullOrWhiteSpace(botToken))
        {
            throw new ArgumentException($"Missing required configuration: {SlackConnectorConfig.BotTokenConfig}");
        }

        // Socket Mode requires App Token
        var useSocketMode = SlackConnectorConfig.DefaultSocketMode;
        if (config.TryGetValue(SlackConnectorConfig.SocketModeConfig, out var socketModeStr) &&
            bool.TryParse(socketModeStr, out var socketMode))
        {
            useSocketMode = socketMode;
        }

        if (useSocketMode &&
            (!config.TryGetValue(SlackConnectorConfig.AppTokenConfig, out var appToken) ||
             string.IsNullOrWhiteSpace(appToken)))
        {
            throw new ArgumentException($"Socket Mode requires {SlackConnectorConfig.AppTokenConfig}");
        }
    }

    public override void Stop()
    {
        // No cleanup needed
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task per connector
        return [new Dictionary<string, string>(_config)];
    }
}
