using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Facebook;

/// <summary>
/// Source task that receives Facebook page events via webhook.
/// </summary>
#pragma warning disable CA2213
public sealed class FacebookSourceTask : SourceTask
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
    private string _pageId = string.Empty;
    private long _messageId;
    private CancellationTokenSource? _cts;
    private Task? _listenerTask;

    private readonly Channel<FacebookWebhookPayload> _eventChannel = Channel.CreateBounded<FacebookWebhookPayload>(
        new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[FacebookConnectorConfig.Topic];
        _verifyToken = config[FacebookConnectorConfig.WebhookVerifyToken];
        _pageId = config[FacebookConnectorConfig.PageId];

        var port = config.TryGetValue(FacebookConnectorConfig.WebhookPort, out var p)
            ? int.Parse(p) : FacebookConnectorConfig.DefaultWebhookPort;
        var path = config.TryGetValue(FacebookConnectorConfig.WebhookPath, out var pa)
            ? pa : FacebookConnectorConfig.DefaultWebhookPath;

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
                var payload = JsonSerializer.Deserialize<FacebookWebhookPayload>(body, JsonOptions);

                if (payload != null)
                {
                    await _eventChannel.Writer.WriteAsync(payload);
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

        while (_eventChannel.Reader.TryRead(out var payload))
        {
            if (payload.Entry == null) continue;

            foreach (var entry in payload.Entry)
            {
                if (entry.Changes == null) continue;

                foreach (var change in entry.Changes)
                {
                    records.Add(CreateSourceRecord(change, entry.Id ?? _pageId));
                }
            }
        }

        if (records.Count == 0)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(TimeSpan.FromSeconds(1));

                if (await _eventChannel.Reader.WaitToReadAsync(cts.Token))
                {
                    while (_eventChannel.Reader.TryRead(out var payload))
                    {
                        if (payload.Entry == null) continue;

                        foreach (var entry in payload.Entry)
                        {
                            if (entry.Changes == null) continue;

                            foreach (var change in entry.Changes)
                            {
                                records.Add(CreateSourceRecord(change, entry.Id ?? _pageId));
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

    private SourceRecord CreateSourceRecord(FacebookChange change, string pageId)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["field"] = change.Field,
            ["value"] = change.Value
        };

        var json = JsonSerializer.Serialize(eventData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(Guid.NewGuid().ToString());

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["page_id"] = pageId
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["field"] = change.Field ?? string.Empty,
                ["offset"] = Interlocked.Increment(ref _messageId)
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["facebook.page.id"] = Encoding.UTF8.GetBytes(pageId),
                ["facebook.field"] = Encoding.UTF8.GetBytes(change.Field ?? string.Empty)
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
    private sealed class FacebookWebhookPayload
    {
        public string? Object { get; set; }
        public List<FacebookEntry>? Entry { get; set; }
    }

    private sealed class FacebookEntry
    {
        public string? Id { get; set; }
        public long Time { get; set; }
        public List<FacebookChange>? Changes { get; set; }
    }

    private sealed class FacebookChange
    {
        public string? Field { get; set; }
        public object? Value { get; set; }
    }
#pragma warning restore CA1812
}
#pragma warning restore CA2213
