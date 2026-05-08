using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Discord;

/// <summary>
/// Source connector that receives messages from Discord via Gateway WebSocket.
/// </summary>
[ConnectorMetadata(
    Name = "discord-source",
    Description = "Receives messages from Discord channels via Gateway WebSocket",
    Author = "Surgewave",
    Tags = "discord, chat, messaging, source, social")]
public sealed class DiscordSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(DiscordConnectorConfig.BotToken, ConfigType.Password, Importance.High,
            "Discord bot token")
        .Define(DiscordConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce messages to", EditorHint.Topic)
        .Define(DiscordConnectorConfig.GuildIds, ConfigType.List, "", Importance.Medium,
            "Comma-separated list of guild (server) IDs to monitor")
        .Define(DiscordConnectorConfig.ChannelIds, ConfigType.List, "", Importance.Medium,
            "Comma-separated list of channel IDs to monitor (empty = all accessible)")
        .Define(DiscordConnectorConfig.MessageTypes, ConfigType.String, DiscordConnectorConfig.DefaultMessageTypes,
            Importance.Medium, "Message types: text, embed, attachment, all", EditorHint.Select, options: ["text", "embed", "attachment", "all"])
        .Define(DiscordConnectorConfig.IncludeBots, ConfigType.Boolean, "false", Importance.Low,
            "Include messages from bots")
        .Define(DiscordConnectorConfig.IncludeReactions, ConfigType.Boolean, "false", Importance.Low,
            "Include reaction events")
        .Define(DiscordConnectorConfig.IncludeEdits, ConfigType.Boolean, "true", Importance.Low,
            "Include message edit events")
        .Define(DiscordConnectorConfig.IncludeDeletes, ConfigType.Boolean, "false", Importance.Low,
            "Include message delete events");

    public override Type TaskClass => typeof(DiscordSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(DiscordConnectorConfig.BotToken, out var token) ||
            string.IsNullOrWhiteSpace(token))
        {
            throw new ArgumentException($"'{DiscordConnectorConfig.BotToken}' is required");
        }

        if (!config.TryGetValue(DiscordConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{DiscordConnectorConfig.Topic}' is required");
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
