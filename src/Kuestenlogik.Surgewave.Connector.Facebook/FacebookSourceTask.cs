using System.Net;
using System.Security.Cryptography;
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

    private const string SignaturePrefix = "sha256=";
    private static readonly TimeSpan EnqueueTimeout = TimeSpan.FromSeconds(10);

    private HttpListener? _listener;
    private string _topic = string.Empty;
    private string _verifyToken = string.Empty;
    private string _pageId = string.Empty;
    private byte[]? _appSecret;
    private long _messageId;
    private CancellationTokenSource? _cts;
    private Task? _listenerTask;

    // Wait (not DropOldest): a delivery is only acknowledged once it is queued, so a
    // backlog turns into backpressure instead of silently discarded events.
    private readonly Channel<FacebookWebhookPayload> _eventChannel = Channel.CreateBounded<FacebookWebhookPayload>(
        new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[FacebookConnectorConfig.Topic];
        _verifyToken = config[FacebookConnectorConfig.WebhookVerifyToken];
        _pageId = config[FacebookConnectorConfig.PageId];
        _appSecret = config.TryGetValue(FacebookConnectorConfig.AppSecret, out var appSecret) && !string.IsNullOrEmpty(appSecret)
            ? Encoding.UTF8.GetBytes(appSecret)
            : null;

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
                using var buffer = new MemoryStream();
                await context.Request.InputStream.CopyToAsync(buffer);
                var body = buffer.ToArray();

                if (!IsSignatureValid(context.Request, body))
                {
                    // Unsigned or forged delivery - refuse it instead of injecting fake events
                    context.Response.StatusCode = 403;
                    return;
                }

                FacebookWebhookPayload? payload;
                try
                {
                    payload = JsonSerializer.Deserialize<FacebookWebhookPayload>(body, JsonOptions);
                }
                catch (JsonException ex)
                {
                    Context?.RaiseError?.Invoke(ex);
                    context.Response.StatusCode = 400;
                    return;
                }

                if (payload != null && !await TryEnqueueAsync(payload))
                {
                    // Backlog: do not acknowledge, so Facebook redelivers instead of the
                    // event being dropped on the floor.
                    context.Response.StatusCode = 503;
                    return;
                }

                context.Response.StatusCode = 200;
            }
            else
            {
                context.Response.StatusCode = 405;
            }
        }
        catch (Exception ex)
        {
            // The handler runs detached from the listener loop; surface the failure
            // instead of losing it in an unobserved task.
            Context?.RaiseError?.Invoke(ex);
        }
        finally
        {
            context.Response.Close();
        }
    }

    private bool IsSignatureValid(HttpListenerRequest request, byte[] body) =>
        IsSignatureValid(_appSecret, request.Headers[FacebookConnectorConfig.SignatureHeader], body);

    /// <summary>
    /// Verifies the X-Hub-Signature-256 header of a delivery against the app secret.
    /// </summary>
    internal static bool IsSignatureValid(byte[]? secret, string? header, byte[] body)
    {
        // Signature verification is opt-in: without an app secret there is nothing to verify against.
        if (secret == null) return true;

        if (string.IsNullOrEmpty(header) || !header.StartsWith(SignaturePrefix, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        byte[] provided;
        try
        {
            provided = Convert.FromHexString(header[SignaturePrefix.Length..]);
        }
        catch (FormatException)
        {
            return false;
        }

        var expected = HMACSHA256.HashData(secret, body);
        return CryptographicOperations.FixedTimeEquals(provided, expected);
    }

    private async Task<bool> TryEnqueueAsync(FacebookWebhookPayload payload)
    {
        if (_eventChannel.Writer.TryWrite(payload)) return true;

        using var cts = new CancellationTokenSource(EnqueueTimeout);
        try
        {
            while (await _eventChannel.Writer.WaitToWriteAsync(cts.Token))
            {
                if (_eventChannel.Writer.TryWrite(payload)) return true;
            }
        }
        catch (OperationCanceledException)
        {
            // Consumer is not draining fast enough - let the caller answer 503
        }

        return false;
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
