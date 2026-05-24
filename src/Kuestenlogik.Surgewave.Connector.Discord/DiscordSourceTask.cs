using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Discord;
using Discord.WebSocket;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Discord;

/// <summary>
/// Task that receives messages from Discord via Gateway WebSocket.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class DiscordSourceTask : SourceTask
{
    private DiscordSocketClient? _client;
    private string _topic = null!;
    private HashSet<ulong> _guildIds = [];
    private HashSet<ulong> _channelIds = [];
    private bool _includeBots;
    private bool _includeReactions;
    private bool _includeEdits;
    private bool _includeDeletes;
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private long _messageId;
    private TaskCompletionSource _readyTcs = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var token = config[DiscordConnectorConfig.BotToken];
        _topic = config[DiscordConnectorConfig.Topic];
        _includeBots = (config.TryGetValue(DiscordConnectorConfig.IncludeBots, out var bots) ? bots : "false") == "true";
        _includeReactions = (config.TryGetValue(DiscordConnectorConfig.IncludeReactions, out var reactions) ? reactions : "false") == "true";
        _includeEdits = (config.TryGetValue(DiscordConnectorConfig.IncludeEdits, out var edits) ? edits : "true") == "true";
        _includeDeletes = (config.TryGetValue(DiscordConnectorConfig.IncludeDeletes, out var deletes) ? deletes : "false") == "true";

        if (config.TryGetValue(DiscordConnectorConfig.GuildIds, out var guilds) && !string.IsNullOrWhiteSpace(guilds))
        {
            _guildIds = guilds.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(ulong.Parse).ToHashSet();
        }

        if (config.TryGetValue(DiscordConnectorConfig.ChannelIds, out var channels) && !string.IsNullOrWhiteSpace(channels))
        {
            _channelIds = channels.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(ulong.Parse).ToHashSet();
        }

        var socketConfig = new DiscordSocketConfig
        {
            GatewayIntents = GatewayIntents.Guilds | GatewayIntents.GuildMessages | GatewayIntents.MessageContent
        };

        if (_includeReactions)
            socketConfig.GatewayIntents |= GatewayIntents.GuildMessageReactions;

        _client = new DiscordSocketClient(socketConfig);
        _client.MessageReceived += OnMessageReceived;
        _client.Ready += OnReady;

        if (_includeEdits)
            _client.MessageUpdated += OnMessageUpdated;
        if (_includeDeletes)
            _client.MessageDeleted += OnMessageDeleted;
        if (_includeReactions)
        {
            _client.ReactionAdded += OnReactionAdded;
            _client.ReactionRemoved += OnReactionRemoved;
        }

        // Start connection in background
        Task.Run(async () =>
        {
            await _client.LoginAsync(TokenType.Bot, token);
            await _client.StartAsync();
        });
    }

    private Task OnReady()
    {
        _readyTcs.TrySetResult();
        return Task.CompletedTask;
    }

    private Task OnMessageReceived(SocketMessage message)
    {
        if (!ShouldProcess(message)) return Task.CompletedTask;

        var record = CreateMessageRecord(message, "message_create");
        _pendingRecords.Enqueue(record);
        return Task.CompletedTask;
    }

    private async Task OnMessageUpdated(Cacheable<IMessage, ulong> before, SocketMessage after, ISocketMessageChannel channel)
    {
        if (!ShouldProcess(after)) return;

        var record = CreateMessageRecord(after, "message_update");
        _pendingRecords.Enqueue(record);
    }

    private Task OnMessageDeleted(Cacheable<IMessage, ulong> message, Cacheable<IMessageChannel, ulong> channel)
    {
        var payload = new
        {
            event_type = "message_delete",
            message_id = message.Id.ToString(),
            channel_id = channel.Id.ToString(),
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "discord" },
            SourceOffset = new Dictionary<string, object> { ["message_id"] = Interlocked.Increment(ref _messageId) },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(message.Id.ToString()),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["discord.event.type"] = Encoding.UTF8.GetBytes("message_delete"),
                ["discord.message.id"] = Encoding.UTF8.GetBytes(message.Id.ToString())
            }
        };

        _pendingRecords.Enqueue(record);
        return Task.CompletedTask;
    }

    private Task OnReactionAdded(Cacheable<IUserMessage, ulong> message, Cacheable<IMessageChannel, ulong> channel, SocketReaction reaction)
    {
        var record = CreateReactionRecord(message.Id, channel.Id, reaction, "reaction_add");
        _pendingRecords.Enqueue(record);
        return Task.CompletedTask;
    }

    private Task OnReactionRemoved(Cacheable<IUserMessage, ulong> message, Cacheable<IMessageChannel, ulong> channel, SocketReaction reaction)
    {
        var record = CreateReactionRecord(message.Id, channel.Id, reaction, "reaction_remove");
        _pendingRecords.Enqueue(record);
        return Task.CompletedTask;
    }

    private bool ShouldProcess(SocketMessage message)
    {
        if (message.Author.IsBot && !_includeBots) return false;
        if (_channelIds.Count > 0 && !_channelIds.Contains(message.Channel.Id)) return false;

        if (_guildIds.Count > 0 && message.Channel is SocketGuildChannel guildChannel)
        {
            if (!_guildIds.Contains(guildChannel.Guild.Id)) return false;
        }

        return true;
    }

    private SourceRecord CreateMessageRecord(SocketMessage message, string eventType)
    {
        var guildId = (message.Channel as SocketGuildChannel)?.Guild.Id;
        var guildName = (message.Channel as SocketGuildChannel)?.Guild.Name;

        var payload = new
        {
            event_type = eventType,
            message_id = message.Id.ToString(),
            channel_id = message.Channel.Id.ToString(),
            channel_name = message.Channel.Name,
            guild_id = guildId?.ToString(),
            guild_name = guildName,
            author_id = message.Author.Id.ToString(),
            author_username = message.Author.Username,
            author_discriminator = message.Author.Discriminator,
            author_is_bot = message.Author.IsBot,
            content = message.Content,
            timestamp = message.Timestamp.ToUnixTimeMilliseconds(),
            attachments = message.Attachments.Select(a => new { a.Id, a.Filename, a.Url, a.Size }).ToList(),
            embeds_count = message.Embeds.Count,
            mentions_count = message.MentionedUsers.Count
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "discord",
                ["channel_id"] = message.Channel.Id.ToString()
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = Interlocked.Increment(ref _messageId),
                ["discord_message_id"] = message.Id.ToString()
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(message.Id.ToString()),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = message.Timestamp,
            Headers = new Dictionary<string, byte[]>
            {
                ["discord.event.type"] = Encoding.UTF8.GetBytes(eventType),
                ["discord.message.id"] = Encoding.UTF8.GetBytes(message.Id.ToString()),
                ["discord.channel.id"] = Encoding.UTF8.GetBytes(message.Channel.Id.ToString()),
                ["discord.author.id"] = Encoding.UTF8.GetBytes(message.Author.Id.ToString())
            }
        };
    }

    private SourceRecord CreateReactionRecord(ulong messageId, ulong channelId, SocketReaction reaction, string eventType)
    {
        var payload = new
        {
            event_type = eventType,
            message_id = messageId.ToString(),
            channel_id = channelId.ToString(),
            user_id = reaction.UserId.ToString(),
            emote = reaction.Emote.Name,
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "discord" },
            SourceOffset = new Dictionary<string, object> { ["message_id"] = Interlocked.Increment(ref _messageId) },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"{messageId}:{reaction.Emote.Name}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = new Dictionary<string, byte[]>
            {
                ["discord.event.type"] = Encoding.UTF8.GetBytes(eventType),
                ["discord.message.id"] = Encoding.UTF8.GetBytes(messageId.ToString())
            }
        };
    }

    public override Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        while (_pendingRecords.TryDequeue(out var record) && records.Count < 100)
        {
            records.Add(record);
        }

        return Task.FromResult<IReadOnlyList<SourceRecord>>(records);
    }

    public override void Stop()
    {
        _client?.StopAsync().GetAwaiter().GetResult();
        _client?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
