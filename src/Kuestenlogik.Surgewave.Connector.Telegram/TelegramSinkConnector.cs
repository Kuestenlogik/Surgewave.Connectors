using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Telegram;

/// <summary>
/// Sink connector that sends messages to Telegram chats.
/// </summary>
[ConnectorMetadata(
    Name = "telegram-sink",
    Description = "Sends messages to Telegram chats via Bot API",
    Author = "Surgewave",
    Tags = "telegram, chat, messaging, sink, bot")]
public sealed class TelegramSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(TelegramConnectorConfig.BotToken, ConfigType.Password, Importance.High,
            "Telegram bot token from @BotFather")
        .Define(TelegramConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume", EditorHint.Topic)
        .Define(TelegramConnectorConfig.DefaultChatId, ConfigType.String, Importance.High,
            "Default chat ID to send messages to")
        .Define(TelegramConnectorConfig.ParseMode, ConfigType.String, TelegramConnectorConfig.DefaultParseMode,
            Importance.Medium, "Parse mode: Markdown, MarkdownV2, HTML")
        .Define(TelegramConnectorConfig.DisableNotification, ConfigType.Boolean, "false", Importance.Low,
            "Disable notification sound")
        .Define(TelegramConnectorConfig.DisableWebPagePreview, ConfigType.Boolean, "false", Importance.Low,
            "Disable web page preview for links");

    public override Type TaskClass => typeof(TelegramSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(TelegramConnectorConfig.BotToken, out var token) ||
            string.IsNullOrWhiteSpace(token))
        {
            throw new ArgumentException($"'{TelegramConnectorConfig.BotToken}' is required");
        }

        if (!config.TryGetValue(TelegramConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{TelegramConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(TelegramConnectorConfig.DefaultChatId, out var chatId) ||
            string.IsNullOrWhiteSpace(chatId))
        {
            throw new ArgumentException($"'{TelegramConnectorConfig.DefaultChatId}' is required");
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
