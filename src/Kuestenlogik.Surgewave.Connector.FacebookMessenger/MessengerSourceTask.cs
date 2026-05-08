using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.FacebookMessenger;

/// <summary>
/// Source task that receives Facebook Messenger messages via webhook.
/// </summary>
#pragma warning disable CA2213
public sealed class MessengerSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    private HttpListener? _listener;
    private string _topic = string.Empty;
    private string _verifyToken = string.Empty;
    private long _messageId;
    private CancellationTokenSource? _cts;
    private Task? _listenerTask;

    private readonly Channel<MessengerWebhookPayload> _messageChannel = Channel.CreateBounded<MessengerWebhookPayload>(
        new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[MessengerConnectorConfig.Topic];
        _verifyToken = config[MessengerConnectorConfig.WebhookVerifyToken];

        var port = config.TryGetValue(MessengerConnectorConfig.WebhookPort, out var p)
            ? int.Parse(p) : MessengerConnectorConfig.DefaultWebhookPort;
        var path = config.TryGetValue(MessengerConnectorConfig.WebhookPath, out var pa)
            ? pa : MessengerConnectorConfig.DefaultWebhookPath;

        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://+:{port}{path}/");
        _listener.Start();

        _cts = new CancellationTokenSource();
        _listenerTask = ListenAsync(_cts.Token);
    }

    private async Task ListenAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && _listener?.IsListening == true)
        {
            try
            {
                var context = await _listener.GetContextAsync();
                _ = HandleRequestAsync(context);
            }
            catch (HttpListenerException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
        }
    }

    private async Task HandleRequestAsync(HttpListenerContext context)
    {
        try
        {
            if (context.Request.HttpMethod == "GET")
            {
                var mode = context.Request.QueryString["hub.mode"];
                var token = context.Request.QueryString["hub.verify_token"];
                var challenge = context.Request.QueryString["hub.challenge"];

                if (mode == "subscribe" && token == _verifyToken)
                {
                    context.Response.StatusCode = 200;
                    var bytes = Encoding.UTF8.GetBytes(challenge ?? "");
                    await context.Response.OutputStream.WriteAsync(bytes);
                }
                else
                {
                    context.Response.StatusCode = 403;
                }
            }
            else if (context.Request.HttpMethod == "POST")
            {
                using var reader = new StreamReader(context.Request.InputStream);
                var body = await reader.ReadToEndAsync();
                var payload = JsonSerializer.Deserialize<MessengerWebhookPayload>(body, JsonOptions);

                if (payload != null)
                {
                    await _messageChannel.Writer.WriteAsync(payload);
                }

                context.Response.StatusCode = 200;
            }
            else
            {
                context.Response.StatusCode = 405;
            }
        }
        finally
        {
            context.Response.Close();
        }
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        while (_messageChannel.Reader.TryRead(out var payload))
        {
            ProcessPayload(payload, records);
        }

        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(TimeSpan.FromSeconds(1));

                if (await _messageChannel.Reader.WaitToReadAsync(cts.Token))
                {
                    while (_messageChannel.Reader.TryRead(out var payload))
                    {
                        ProcessPayload(payload, records);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        return records;
    }

    private void ProcessPayload(MessengerWebhookPayload payload, List<SourceRecord> records)
    {
        if (payload.Entry == null) return;

        foreach (var entry in payload.Entry)
        {
            if (entry.Messaging == null) continue;

            foreach (var messaging in entry.Messaging)
            {
                if (messaging.Message == null) continue;
                records.Add(CreateSourceRecord(messaging, entry.Id ?? "unknown"));
            }
        }
    }

    private SourceRecord CreateSourceRecord(MessengerMessaging messaging, string pageId)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["sender_id"] = messaging.Sender?.Id,
            ["recipient_id"] = messaging.Recipient?.Id,
            ["timestamp"] = messaging.Timestamp,
            ["message_id"] = messaging.Message?.Mid,
            ["text"] = messaging.Message?.Text,
            ["quick_reply"] = messaging.Message?.QuickReply?.Payload,
            ["attachments"] = messaging.Message?.Attachments
        };

        var json = JsonSerializer.Serialize(eventData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(messaging.Message?.Mid ?? Guid.NewGuid().ToString());

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["page_id"] = pageId
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = messaging.Message?.Mid ?? string.Empty,
                ["timestamp"] = messaging.Timestamp,
                ["offset"] = Interlocked.Increment(ref _messageId)
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(messaging.Timestamp),
            Headers = new Dictionary<string, byte[]>
            {
                ["messenger.sender.id"] = Encoding.UTF8.GetBytes(messaging.Sender?.Id ?? string.Empty),
                ["messenger.message.id"] = Encoding.UTF8.GetBytes(messaging.Message?.Mid ?? string.Empty),
                ["messenger.page.id"] = Encoding.UTF8.GetBytes(pageId)
            }
        };
    }

    public override void Stop()
    {
        _cts?.Cancel();
        _listener?.Stop();
        _listener?.Close();
        _listener = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _cts?.Dispose();
        }
        base.Dispose(disposing);
    }

#pragma warning disable CA1812
    private sealed class MessengerWebhookPayload
    {
        public string? Object { get; set; }
        public List<MessengerEntry>? Entry { get; set; }
    }

    private sealed class MessengerEntry
    {
        public string? Id { get; set; }
        public long Time { get; set; }
        public List<MessengerMessaging>? Messaging { get; set; }
    }

    private sealed class MessengerMessaging
    {
        public MessengerUser? Sender { get; set; }
        public MessengerUser? Recipient { get; set; }
        public long Timestamp { get; set; }
        public MessengerMessage? Message { get; set; }
    }

    private sealed class MessengerUser
    {
        public string? Id { get; set; }
    }

    private sealed class MessengerMessage
    {
        public string? Mid { get; set; }
        public string? Text { get; set; }
        public MessengerQuickReply? QuickReply { get; set; }
        public List<object>? Attachments { get; set; }
    }

    private sealed class MessengerQuickReply
    {
        public string? Payload { get; set; }
    }
#pragma warning restore CA1812
}
#pragma warning restore CA2213
