using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.WhatsApp;

/// <summary>
/// Source task that receives WhatsApp messages via webhook HTTP listener.
/// </summary>
#pragma warning disable CA2213
public sealed class WhatsAppSourceTask : SourceTask
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

    private readonly Channel<WhatsAppWebhookPayload> _messageChannel = Channel.CreateBounded<WhatsAppWebhookPayload>(
        new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[WhatsAppConnectorConfig.Topic];
        _verifyToken = config[WhatsAppConnectorConfig.WebhookVerifyToken];

        var port = config.TryGetValue(WhatsAppConnectorConfig.WebhookPort, out var p)
            ? int.Parse(p) : WhatsAppConnectorConfig.DefaultWebhookPort;
        var path = config.TryGetValue(WhatsAppConnectorConfig.WebhookPath, out var pa)
            ? pa : WhatsAppConnectorConfig.DefaultWebhookPath;

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
                // Webhook verification
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
                var payload = JsonSerializer.Deserialize<WhatsAppWebhookPayload>(body, JsonOptions);

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
            if (payload.Entry == null) continue;

            foreach (var entry in payload.Entry)
            {
                if (entry.Changes == null) continue;

                foreach (var change in entry.Changes)
                {
                    if (change.Value?.Messages == null) continue;

                    foreach (var message in change.Value.Messages)
                    {
                        records.Add(CreateSourceRecord(message, entry.Id ?? "unknown"));
                    }
                }
            }
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
                        if (payload.Entry == null) continue;

                        foreach (var entry in payload.Entry)
                        {
                            if (entry.Changes == null) continue;

                            foreach (var change in entry.Changes)
                            {
                                if (change.Value?.Messages == null) continue;

                                foreach (var message in change.Value.Messages)
                                {
                                    records.Add(CreateSourceRecord(message, entry.Id ?? "unknown"));
                                }
                            }
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        return records;
    }

    private SourceRecord CreateSourceRecord(WhatsAppMessage message, string businessId)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["id"] = message.Id,
            ["from"] = message.From,
            ["timestamp"] = message.Timestamp,
            ["type"] = message.Type,
            ["text"] = message.Text?.Body
        };

        var json = JsonSerializer.Serialize(eventData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(message.Id ?? Guid.NewGuid().ToString());

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["business_id"] = businessId
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = message.Id ?? string.Empty,
                ["timestamp"] = message.Timestamp ?? string.Empty,
                ["offset"] = Interlocked.Increment(ref _messageId)
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = ParseTimestamp(message.Timestamp),
            Headers = new Dictionary<string, byte[]>
            {
                ["whatsapp.from"] = Encoding.UTF8.GetBytes(message.From ?? string.Empty),
                ["whatsapp.message.id"] = Encoding.UTF8.GetBytes(message.Id ?? string.Empty),
                ["whatsapp.type"] = Encoding.UTF8.GetBytes(message.Type ?? string.Empty)
            }
        };
    }

    private static DateTimeOffset ParseTimestamp(string? ts)
    {
        if (string.IsNullOrEmpty(ts) || !long.TryParse(ts, out var seconds))
            return DateTimeOffset.UtcNow;
        return DateTimeOffset.FromUnixTimeSeconds(seconds);
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
    private sealed class WhatsAppWebhookPayload
    {
        public string? Object { get; set; }
        public List<WhatsAppEntry>? Entry { get; set; }
    }

    private sealed class WhatsAppEntry
    {
        public string? Id { get; set; }
        public List<WhatsAppChange>? Changes { get; set; }
    }

    private sealed class WhatsAppChange
    {
        public string? Field { get; set; }
        public WhatsAppValue? Value { get; set; }
    }

    private sealed class WhatsAppValue
    {
        public string? MessagingProduct { get; set; }
        public List<WhatsAppMessage>? Messages { get; set; }
    }

    private sealed class WhatsAppMessage
    {
        public string? Id { get; set; }
        public string? From { get; set; }
        public string? Timestamp { get; set; }
        public string? Type { get; set; }
        public WhatsAppText? Text { get; set; }
    }

    private sealed class WhatsAppText
    {
        public string? Body { get; set; }
    }
#pragma warning restore CA1812
}
#pragma warning restore CA2213
