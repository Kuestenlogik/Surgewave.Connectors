using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Slack;

/// <summary>
/// Sink connector that posts messages to Slack via the Web API.
/// </summary>
public class SlackSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(SlackSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Topics
        .Define(SlackConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)

        // Slack API
        .Define(SlackConnectorConfig.BotTokenConfig, ConfigType.Password, Importance.High,
            "Slack Bot User OAuth Token (xoxb-...)")

        // Channel settings
        .Define(SlackConnectorConfig.DefaultChannelConfig, ConfigType.String, Importance.High,
            "Default channel to post messages to (channel ID or name)")
        .Define(SlackConnectorConfig.ChannelFieldConfig, ConfigType.String, Importance.Medium,
            "JSON field in record value containing target channel (overrides default)")

        // Message content
        .Define(SlackConnectorConfig.TextFieldConfig, ConfigType.String, Importance.Medium,
            "JSON field in record value containing message text")
        .Define(SlackConnectorConfig.TextTemplateConfig, ConfigType.String, SlackConnectorConfig.DefaultTextTemplate, Importance.Medium,
            "Template for message text. Use ${value} for full value, ${key} for key, ${field.name} for JSON fields", EditorHint.Multiline)
        .Define(SlackConnectorConfig.BlocksFieldConfig, ConfigType.String, Importance.Low,
            "JSON field containing Block Kit blocks array")
        .Define(SlackConnectorConfig.AttachmentsFieldConfig, ConfigType.String, Importance.Low,
            "JSON field containing attachments array")

        // Threading
        .Define(SlackConnectorConfig.ThreadTsFieldConfig, ConfigType.String, Importance.Low,
            "JSON field containing thread timestamp for replies")

        // Message appearance
        .Define(SlackConnectorConfig.UsernameConfig, ConfigType.String, Importance.Low,
            "Custom username for messages")
        .Define(SlackConnectorConfig.IconEmojiConfig, ConfigType.String, Importance.Low,
            "Emoji icon for messages (e.g., :robot_face:)")
        .Define(SlackConnectorConfig.IconUrlConfig, ConfigType.String, Importance.Low,
            "URL for custom icon")

        // Formatting
        .Define(SlackConnectorConfig.MarkdownConfig, ConfigType.Boolean, SlackConnectorConfig.DefaultMarkdown, Importance.Low,
            "Enable markdown formatting in messages")
        .Define(SlackConnectorConfig.UnfurlLinksConfig, ConfigType.Boolean, SlackConnectorConfig.DefaultUnfurlLinks, Importance.Low,
            "Unfurl text-based links")
        .Define(SlackConnectorConfig.UnfurlMediaConfig, ConfigType.Boolean, SlackConnectorConfig.DefaultUnfurlMedia, Importance.Low,
            "Unfurl media links")

        // Reactions
        .Define(SlackConnectorConfig.AddReactionConfig, ConfigType.Boolean, false, Importance.Low,
            "Add reaction to message instead of posting")
        .Define(SlackConnectorConfig.ReactionFieldConfig, ConfigType.String, Importance.Low,
            "JSON field containing reaction emoji name")
        .Define(SlackConnectorConfig.DefaultReactionConfig, ConfigType.String, Importance.Low,
            "Default reaction emoji if field not found")

        // Behavior
        .Define(SlackConnectorConfig.BatchSizeConfig, ConfigType.Int, SlackConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Maximum number of messages to send per batch")
        .Define(SlackConnectorConfig.RetryCountConfig, ConfigType.Int, SlackConnectorConfig.DefaultRetryCount, Importance.Low,
            "Number of retries for failed API calls")
        .Define(SlackConnectorConfig.RetryDelayMsConfig, ConfigType.Int, SlackConnectorConfig.DefaultRetryDelayMs, Importance.Low,
            "Delay between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Validate required config
        if (!config.TryGetValue(SlackConnectorConfig.TopicsConfig, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"Missing required configuration: {SlackConnectorConfig.TopicsConfig}");
        }

        if (!config.TryGetValue(SlackConnectorConfig.BotTokenConfig, out var botToken) ||
            string.IsNullOrWhiteSpace(botToken))
        {
            throw new ArgumentException($"Missing required configuration: {SlackConnectorConfig.BotTokenConfig}");
        }

        // Require either default channel or channel field
        var hasDefaultChannel = config.TryGetValue(SlackConnectorConfig.DefaultChannelConfig, out var defaultChannel) &&
                                !string.IsNullOrWhiteSpace(defaultChannel);
        var hasChannelField = config.TryGetValue(SlackConnectorConfig.ChannelFieldConfig, out var channelField) &&
                              !string.IsNullOrWhiteSpace(channelField);

        if (!hasDefaultChannel && !hasChannelField)
        {
            throw new ArgumentException(
                $"Either {SlackConnectorConfig.DefaultChannelConfig} or {SlackConnectorConfig.ChannelFieldConfig} must be configured");
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
