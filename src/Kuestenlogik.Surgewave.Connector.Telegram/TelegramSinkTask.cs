using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Kuestenlogik.Surgewave.Connector.Telegram;

/// <summary>
/// Task that sends messages to Telegram chats.
/// </summary>
public sealed class TelegramSinkTask : SinkTask
{
    private TelegramBotClient? _client;
    private long _defaultChatId;
    private ParseMode _parseMode;
    private bool _disableNotification;
    private bool _disableWebPagePreview;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var token = config[TelegramConnectorConfig.BotToken];
        _defaultChatId = long.Parse(config[TelegramConnectorConfig.DefaultChatId]);
        _disableNotification = (config.TryGetValue(TelegramConnectorConfig.DisableNotification, out var disableNotif) ? disableNotif : "false") == "true";
        _disableWebPagePreview = (config.TryGetValue(TelegramConnectorConfig.DisableWebPagePreview, out var disablePreview) ? disablePreview : "false") == "true";

        var parseModeStr = config.TryGetValue(TelegramConnectorConfig.ParseMode, out var parseMode) ? parseMode : "Markdown";
        _parseMode = parseModeStr?.ToLowerInvariant() switch
        {
            "html" => ParseMode.Html,
            "markdownv2" => ParseMode.MarkdownV2,
            _ => ParseMode.Markdown
        };

        _client = new TelegramBotClient(token);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            // Get target chat from headers or use default
            var chatId = _defaultChatId;
            if (record.Headers?.TryGetValue("telegram.chat.id", out var chatBytes) == true)
            {
                if (long.TryParse(Encoding.UTF8.GetString(chatBytes), out var parsed))
                    chatId = parsed;
            }

            var text = Encoding.UTF8.GetString(record.Value);

            // Check for reply_to
            int? replyToMessageId = null;
            if (record.Headers?.TryGetValue("telegram.reply.to", out var replyBytes) == true)
            {
                if (int.TryParse(Encoding.UTF8.GetString(replyBytes), out var replyId))
                    replyToMessageId = replyId;
            }

            await _client!.SendMessage(
                chatId: new ChatId(chatId),
                text: text,
                parseMode: _parseMode,
                disableNotification: _disableNotification,
                linkPreviewOptions: _disableWebPagePreview ? new LinkPreviewOptions { IsDisabled = true } : null,
                replyParameters: replyToMessageId.HasValue ? new ReplyParameters { MessageId = replyToMessageId.Value } : null,
                cancellationToken: cancellationToken);
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);
    }
}
