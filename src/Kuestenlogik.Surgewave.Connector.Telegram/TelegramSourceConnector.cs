using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Telegram;

/// <summary>
/// Source connector that receives messages from Telegram via Bot API.
/// </summary>
[ConnectorMetadata(
    Name = "telegram-source",
    Description = "Receives messages from Telegram via Bot API polling or webhook",
    Author = "Surgewave",
    Tags = "telegram, chat, messaging, source, bot")]
public sealed class TelegramSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(TelegramConnectorConfig.BotToken, ConfigType.Password, Importance.High,
            "Telegram bot token from @BotFather")
        .Define(TelegramConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce messages to", EditorHint.Topic)
        .Define(TelegramConnectorConfig.ChatIds, ConfigType.List, "", Importance.Medium,
            "Comma-separated list of chat IDs to monitor (empty = all)")
        .Define(TelegramConnectorConfig.IncludeGroups, ConfigType.Boolean, "true", Importance.Medium,
            "Include group messages")
        .Define(TelegramConnectorConfig.IncludeChannels, ConfigType.Boolean, "true", Importance.Medium,
            "Include channel messages")
        .Define(TelegramConnectorConfig.IncludePrivate, ConfigType.Boolean, "true", Importance.Medium,
            "Include private messages")
        .Define(TelegramConnectorConfig.MessageTypes, ConfigType.String, TelegramConnectorConfig.DefaultMessageTypes,
            Importance.Medium, "Message types: text, photo, video, document, all")
        .Define(TelegramConnectorConfig.PollingMode, ConfigType.String, TelegramConnectorConfig.DefaultPollingMode,
            Importance.Medium, "Polling mode: long-polling, webhook");

    public override Type TaskClass => typeof(TelegramSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(TelegramConnectorConfig.BotToken, out var token) ||
            string.IsNullOrWhiteSpace(token))
        {
            throw new ArgumentException($"'{TelegramConnectorConfig.BotToken}' is required");
        }

        if (!config.TryGetValue(TelegramConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{TelegramConnectorConfig.Topic}' is required");
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
