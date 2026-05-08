using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Telegram.Bot;
using Telegram.Bot.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Kuestenlogik.Surgewave.Connector.Telegram;

/// <summary>
/// Task that receives messages from Telegram via Bot API.
/// </summary>
public sealed class TelegramSourceTask : SourceTask
{
    private TelegramBotClient? _client;
    private string _topic = null!;
    private HashSet<long> _chatIds = [];
    private bool _includeGroups;
    private bool _includeChannels;
    private bool _includePrivate;
    private string _messageTypes = "all";
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private CancellationTokenSource? _pollingCts;
    private long _messageId;

    public override string Version => "1.0.0";

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

        _client = new TelegramBotClient(token);
        _pollingCts = new CancellationTokenSource();

        // Start polling
        _client.StartReceiving(
            updateHandler: HandleUpdateAsync,
            errorHandler: HandleErrorAsync,
            receiverOptions: new ReceiverOptions
            {
                AllowedUpdates = [UpdateType.Message, UpdateType.EditedMessage, UpdateType.ChannelPost, UpdateType.EditedChannelPost]
            },
            cancellationToken: _pollingCts.Token);
    }

    private Task HandleUpdateAsync(ITelegramBotClient client, Update update, CancellationToken cancellationToken)
    {
        var message = update.Message ?? update.EditedMessage ?? update.ChannelPost ?? update.EditedChannelPost;
        if (message == null) return Task.CompletedTask;

        if (!ShouldProcess(message)) return Task.CompletedTask;

        var eventType = update.Type switch
        {
            UpdateType.Message => "message",
            UpdateType.EditedMessage => "message_edit",
            UpdateType.ChannelPost => "channel_post",
            UpdateType.EditedChannelPost => "channel_post_edit",
            _ => "unknown"
        };

        var record = CreateMessageRecord(message, eventType);
        _pendingRecords.Enqueue(record);

        return Task.CompletedTask;
    }

    private Task HandleErrorAsync(ITelegramBotClient client, Exception exception, HandleErrorSource source, CancellationToken cancellationToken)
    {
        // Log error and continue
        return Task.CompletedTask;
    }

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

    private SourceRecord CreateMessageRecord(Message message, string eventType)
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
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "telegram",
                ["chat_id"] = message.Chat.Id.ToString()
            },
            SourceOffset = new Dictionary<string, object>
            {
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
        _pollingCts?.Cancel();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _pollingCts?.Dispose();
        }
        base.Dispose(disposing);
    }
}
