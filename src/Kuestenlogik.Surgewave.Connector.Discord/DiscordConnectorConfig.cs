namespace Kuestenlogik.Surgewave.Connector.Discord;

/// <summary>
/// Configuration constants for Discord connector.
/// </summary>
public static class DiscordConnectorConfig
{
    // Authentication
    public const string BotToken = "discord.bot.token";

    // Source settings
    public const string Topic = "topic";
    public const string GuildIds = "discord.guild.ids";
    public const string ChannelIds = "discord.channel.ids";
    public const string MessageTypes = "discord.message.types";  // text, embed, attachment, all
    public const string IncludeBots = "discord.include.bots";
    public const string IncludeReactions = "discord.include.reactions";
    public const string IncludeEdits = "discord.include.edits";
    public const string IncludeDeletes = "discord.include.deletes";

    // Sink settings
    public const string Topics = "topics";
    public const string DefaultChannelId = "discord.default.channel.id";
    public const string MessageFormat = "discord.message.format";  // text, embed
    public const string EmbedColor = "discord.embed.color";
    public const string EmbedTitle = "discord.embed.title";

    // Defaults
    public const string DefaultMessageTypes = "all";
    public const string DefaultMessageFormat = "text";
    public const int DefaultEmbedColor = 0x5865F2; // Discord Blurple
}
