using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Kuestenlogik.Surgewave.Connector.Telegram;

/// <summary>
/// Task that receives messages from Telegram via Bot API long polling.
/// </summary>
public sealed class TelegramSourceTask : SourceTask
{
    private const int MaxUpdatesPerPoll = 100;
    private const int LongPollTimeoutSeconds = 5;
    private const int ErrorBackoffMs = 1000;

    private static readonly UpdateType[] AllowedUpdates =
        [UpdateType.Message, UpdateType.EditedMessage, UpdateType.ChannelPost, UpdateType.EditedChannelPost];

    private readonly ITelegramBotClient? _injectedClient;
    private ITelegramBotClient? _client;
    private string _topic = null!;
    private HashSet<long> _chatIds = [];
    private bool _includeGroups;
    private bool _includeChannels;
    private bool _includePrivate;
    private string _messageTypes = "all";
    private readonly Dictionary<string, object> _sourcePartition = [];

    /// <summary>Highest update id that Surgewave committed - only this is acknowledged to Telegram.</summary>
    private int _committedUpdateId;

    /// <summary>Highest update id handed to the framework by the batch in flight.</summary>
    private int _polledUpdateId;

    private long _messageId;

    public override string Version => "1.0.0";

    public TelegramSourceTask()
    {
    }

    /// <summary>
    /// Long-polls through a caller-supplied Bot API client instead of opening one of its own.
    /// </summary>
    internal TelegramSourceTask(ITelegramBotClient client) => _injectedClient = client;

    public override void Start(IDictionary<string, string> config)
    {
        var token = config[TelegramConnectorConfig.BotToken];
        _topic = config[TelegramConnectorConfig.Topic];
        _includeGroups = (config.TryGetValue(TelegramConnectorConfig.IncludeGroups, out var includeGroups) ? includeGroups : "true") == "true";
        _includeChannels = (config.TryGetValue(TelegramConnectorConfig.IncludeChannels, out var includeChannels) ? includeChannels : "true") == "true";
        _includePrivate = (config.TryGetValue(TelegramConnectorConfig.IncludePrivate, out var includePrivate) ? includePrivate : "true") == "true";
        _messageTypes = config.TryGetValue(TelegramConnectorConfig.MessageTypes, out var messageTypes) ? messageTypes : "all";

        if (config.TryGetValue(TelegramConnectorConfig.ChatIds, out var chats) && !string.IsNullOrWhiteSpace(chats))
        {
            _chatIds = chats.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(long.Parse).ToHashSet();
        }

        _client = _injectedClient ?? new TelegramBotClient(token);

        // The getUpdates cursor belongs to the bot, not to a single chat. The bot id is the
        // public part of the token, so it identifies the partition without storing the secret.
        var separator = token.IndexOf(':');
        _sourcePartition[TelegramConnectorConfig.PartitionSource] = "telegram";
        _sourcePartition[TelegramConnectorConfig.PartitionBotId] = separator > 0 ? token[..separator] : "unknown";

        _committedUpdateId = ReadStoredUpdateId();
        _polledUpdateId = _committedUpdateId;
    }

    private int ReadStoredUpdateId()
    {
        var stored = Context?.OffsetStorageReader?.Offset(_sourcePartition);

        if (stored != null &&
            stored.TryGetValue(TelegramConnectorConfig.OffsetUpdateId, out var raw) &&
            int.TryParse(raw?.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var updateId))
        {
            return updateId;
        }

        return 0;
    }

    private static string EventTypeOf(Update update) => update.Type switch
    {
        UpdateType.Message => "message",
        UpdateType.EditedMessage => "message_edit",
        UpdateType.ChannelPost => "channel_post",
        UpdateType.EditedChannelPost => "channel_post_edit",
        _ => "unknown"
    };

    private bool ShouldProcess(Message message)
    {
        // Check chat filter
        if (_chatIds.Count > 0 && !_chatIds.Contains(message.Chat.Id))
            return false;

        // Check chat type
        var chatType = message.Chat.Type;
        if (chatType == ChatType.Group || chatType == ChatType.Supergroup)
        {
            if (!_includeGroups) return false;
        }
        else if (chatType == ChatType.Channel)
        {
            if (!_includeChannels) return false;
        }
        else if (chatType == ChatType.Private)
        {
            if (!_includePrivate) return false;
        }

        // Check message type
        if (_messageTypes != "all")
        {
            var types = _messageTypes.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var hasText = !string.IsNullOrEmpty(message.Text);
            var hasPhoto = message.Photo != null;
            var hasVideo = message.Video != null;
            var hasDocument = message.Document != null;

            if (types.Contains("text") && hasText) return true;
            if (types.Contains("photo") && hasPhoto) return true;
            if (types.Contains("video") && hasVideo) return true;
            if (types.Contains("document") && hasDocument) return true;
            return false;
        }

        return true;
    }

    private SourceRecord CreateMessageRecord(Message message, string eventType, int updateId)
    {
        var payload = new
        {
            event_type = eventType,
            message_id = message.MessageId,
            chat_id = message.Chat.Id,
            chat_type = message.Chat.Type.ToString(),
            chat_title = message.Chat.Title ?? message.Chat.Username,
            from_id = message.From?.Id,
            from_username = message.From?.Username,
            from_first_name = message.From?.FirstName,
            from_last_name = message.From?.LastName,
            from_is_bot = message.From?.IsBot,
            text = message.Text ?? message.Caption,
            date = new DateTimeOffset(message.Date).ToUnixTimeSeconds(),
            has_photo = message.Photo != null,
            has_video = message.Video != null,
            has_document = message.Document != null,
            has_audio = message.Audio != null,
            has_voice = message.Voice != null,
            reply_to_message_id = message.ReplyToMessage?.MessageId
        };

        var headers = new Dictionary<string, byte[]>
        {
            ["telegram.event.type"] = Encoding.UTF8.GetBytes(eventType),
            ["telegram.message.id"] = Encoding.UTF8.GetBytes(message.MessageId.ToString()),
            ["telegram.chat.id"] = Encoding.UTF8.GetBytes(message.Chat.Id.ToString()),
            ["telegram.chat.type"] = Encoding.UTF8.GetBytes(message.Chat.Type.ToString())
        };

        if (message.From != null)
        {
            headers["telegram.from.id"] = Encoding.UTF8.GetBytes(message.From.Id.ToString());
        }

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>(_sourcePartition),
            SourceOffset = new Dictionary<string, object>
            {
                [TelegramConnectorConfig.OffsetUpdateId] = updateId,
                ["message_id"] = Interlocked.Increment(ref _messageId),
                ["telegram_message_id"] = message.MessageId
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes($"{message.Chat.Id}:{message.MessageId}"),
            Value = JsonSerializer.SerializeToUtf8Bytes(payload),
            Timestamp = message.Date,
            Headers = headers
        };
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        var client = _client;
        if (client == null) return records;

        try
        {
            // Telegram acknowledges everything below the requested offset, so only the committed
            // update id is passed on: anything not committed yet is redelivered after a crash.
            var updates = await client.GetUpdates(
                offset: _committedUpdateId == 0 ? null : _committedUpdateId + 1,
                limit: MaxUpdatesPerPoll,
                timeout: LongPollTimeoutSeconds,
                allowedUpdates: AllowedUpdates,
                cancellationToken: cancellationToken);

            foreach (var update in updates)
            {
                if (update.Id > _polledUpdateId)
                    _polledUpdateId = update.Id;

                var message = update.Message ?? update.EditedMessage ?? update.ChannelPost ?? update.EditedChannelPost;
                if (message == null) continue;

                if (!ShouldProcess(message)) continue;

                records.Add(CreateMessageRecord(message, EventTypeOf(update), update.Id));
            }

            // Filtered-out updates produce no record and would otherwise be redelivered forever.
            if (records.Count == 0)
                _committedUpdateId = _polledUpdateId;
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }
        catch (Exception ex)
        {
            // A rejected bot token or a transient Bot API failure must be visible instead of
            // silently producing nothing.
            Context?.RaiseError?.Invoke(ex);

            try
            {
                await Task.Delay(ErrorBackoffMs, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Normal cancellation
            }
        }

        return records;
    }

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        if (record.SourceOffset.TryGetValue(TelegramConnectorConfig.OffsetUpdateId, out var raw) &&
            int.TryParse(raw?.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var updateId) &&
            updateId > _committedUpdateId)
        {
            _committedUpdateId = updateId;
        }
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        // The whole polled batch is durable, so every update it covered may be acknowledged.
        if (_polledUpdateId > _committedUpdateId)
            _committedUpdateId = _polledUpdateId;

        return Task.CompletedTask;
    }

    public override void Stop()
    {
    }
}
