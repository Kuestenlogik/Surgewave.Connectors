using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.LinkedIn;

/// <summary>
/// Source task that receives LinkedIn events via webhook.
/// </summary>
#pragma warning disable CA2213
public sealed class LinkedInSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    private HttpListener? _listener;
    private string _topic = string.Empty;
    private string _verifyToken = string.Empty;
    private string? _organizationId;
    private bool _includeShares = true;
    private bool _includeMentions = true;
    private long _messageId;
    private CancellationTokenSource? _cts;
    private Task? _listenerTask;

    private readonly Channel<LinkedInWebhookPayload> _eventChannel = Channel.CreateBounded<LinkedInWebhookPayload>(
        new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false
        });

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[LinkedInConnectorConfig.Topic];
        _verifyToken = config[LinkedInConnectorConfig.WebhookVerifyToken];
        _organizationId = config.TryGetValue(LinkedInConnectorConfig.OrganizationId, out var oid) ? oid : null;

        _includeShares = !config.TryGetValue(LinkedInConnectorConfig.IncludeShares, out var ish) || ish != "false";
        _includeMentions = !config.TryGetValue(LinkedInConnectorConfig.IncludeMentions, out var im) || im != "false";

        var port = config.TryGetValue(LinkedInConnectorConfig.WebhookPort, out var p)
            ? int.Parse(p) : LinkedInConnectorConfig.DefaultWebhookPort;
        var path = config.TryGetValue(LinkedInConnectorConfig.WebhookPath, out var pa)
            ? pa : LinkedInConnectorConfig.DefaultWebhookPath;

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
                // LinkedIn uses challengeCode for verification
                var challenge = context.Request.QueryString["challengeCode"];
                var validationToken = context.Request.QueryString["validationToken"];

                if (!string.IsNullOrEmpty(challenge))
                {
                    context.Response.StatusCode = 200;
                    var response = JsonSerializer.Serialize(new { challengeCode = challenge });
                    var bytes = Encoding.UTF8.GetBytes(response);
                    context.Response.ContentType = "application/json";
                    await context.Response.OutputStream.WriteAsync(bytes);
                }
                else if (!string.IsNullOrEmpty(validationToken))
                {
                    context.Response.StatusCode = 200;
                    var bytes = Encoding.UTF8.GetBytes(validationToken);
                    await context.Response.OutputStream.WriteAsync(bytes);
                }
                else
                {
                    context.Response.StatusCode = 400;
                }
            }
            else if (context.Request.HttpMethod == "POST")
            {
                using var reader = new StreamReader(context.Request.InputStream);
                var body = await reader.ReadToEndAsync();
                var payload = JsonSerializer.Deserialize<LinkedInWebhookPayload>(body, JsonOptions);

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
            ProcessPayload(payload, records);
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

    private void ProcessPayload(LinkedInWebhookPayload payload, List<SourceRecord> records)
    {
        if (payload.EventType == "SHARE" && !_includeShares) return;
        if (payload.EventType == "MENTION" && !_includeMentions) return;

        records.Add(CreateSourceRecord(payload));
    }

    private SourceRecord CreateSourceRecord(LinkedInWebhookPayload payload)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["event_type"] = payload.EventType,
            ["resource_urn"] = payload.ResourceUrn,
            ["owner_urn"] = payload.OwnerUrn,
            ["created_at"] = payload.CreatedAt,
            ["data"] = payload.Data
        };

        var json = JsonSerializer.Serialize(eventData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(payload.ResourceUrn ?? Guid.NewGuid().ToString());

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["owner_urn"] = payload.OwnerUrn ?? _organizationId ?? "unknown"
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["resource_urn"] = payload.ResourceUrn ?? string.Empty,
                ["created_at"] = payload.CreatedAt,
                ["offset"] = Interlocked.Increment(ref _messageId)
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(payload.CreatedAt),
            Headers = new Dictionary<string, byte[]>
            {
                ["linkedin.event.type"] = Encoding.UTF8.GetBytes(payload.EventType ?? string.Empty),
                ["linkedin.owner.urn"] = Encoding.UTF8.GetBytes(payload.OwnerUrn ?? string.Empty),
                ["linkedin.resource.urn"] = Encoding.UTF8.GetBytes(payload.ResourceUrn ?? string.Empty)
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
    private sealed class LinkedInWebhookPayload
    {
        public string? EventType { get; set; }
        public string? ResourceUrn { get; set; }
        public string? OwnerUrn { get; set; }
        public long CreatedAt { get; set; }
        public object? Data { get; set; }
    }
#pragma warning restore CA1812
}
#pragma warning restore CA2213
