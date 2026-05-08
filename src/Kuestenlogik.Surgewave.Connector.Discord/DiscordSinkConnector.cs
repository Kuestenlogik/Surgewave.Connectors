using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Discord;

/// <summary>
/// Sink connector that sends messages to Discord channels.
/// </summary>
[ConnectorMetadata(
    Name = "discord-sink",
    Description = "Sends messages to Discord channels via REST API",
    Author = "Surgewave",
    Tags = "discord, chat, messaging, sink, social")]
public sealed class DiscordSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(DiscordConnectorConfig.BotToken, ConfigType.Password, Importance.High,
            "Discord bot token")
        .Define(DiscordConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume", EditorHint.Topic)
        .Define(DiscordConnectorConfig.DefaultChannelId, ConfigType.String, Importance.High,
            "Default Discord channel ID to send messages to")
        .Define(DiscordConnectorConfig.MessageFormat, ConfigType.String, DiscordConnectorConfig.DefaultMessageFormat,
            Importance.Medium, "Message format: text, embed", EditorHint.Select, options: ["text", "embed"])
        .Define(DiscordConnectorConfig.EmbedColor, ConfigType.Int, DiscordConnectorConfig.DefaultEmbedColor.ToString(),
            Importance.Low, "Embed color (decimal)")
        .Define(DiscordConnectorConfig.EmbedTitle, ConfigType.String, "", Importance.Low,
            "Embed title template (supports ${topic})");

    public override Type TaskClass => typeof(DiscordSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(DiscordConnectorConfig.BotToken, out var token) ||
            string.IsNullOrWhiteSpace(token))
        {
            throw new ArgumentException($"'{DiscordConnectorConfig.BotToken}' is required");
        }

        if (!config.TryGetValue(DiscordConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{DiscordConnectorConfig.Topics}' is required");
        }

        if (!config.TryGetValue(DiscordConnectorConfig.DefaultChannelId, out var channel) ||
            string.IsNullOrWhiteSpace(channel))
        {
            throw new ArgumentException($"'{DiscordConnectorConfig.DefaultChannelId}' is required");
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
