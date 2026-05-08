using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sap.EventMesh;

/// <summary>
/// Task that reads events from SAP Event Mesh.
/// </summary>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via reflection by connector framework")]
[SuppressMessage("Performance", "CA1823:Avoid unused private fields", Justification = "Fields used by CloudEvents")]
[SuppressMessage("Performance", "CA1852:Seal internal types", Justification = "Public connector task")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "HttpClient disposed in Dispose()")]
public sealed class EventMeshSourceTask : SourceTask
{
    private HttpClient? _httpClient;
    private string _serviceUrl = null!;
    private string _tokenUrl = null!;
    private string _clientId = null!;
    private string _clientSecret = null!;
    private string _queueName = null!;
    private string _topic = null!;
    private int _pollIntervalMs;
    private int _maxMessages;
    private string _ackMode = null!;
    private DateTime _lastPoll = DateTime.MinValue;
    private string? _accessToken;
    private DateTime _tokenExpiry = DateTime.MinValue;
    private long _messageId;
    private readonly List<string> _pendingAcks = new();
    private readonly JsonEventFormatter _formatter = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[EventMeshConnectorConfig.Topic];
        _serviceUrl = config[EventMeshConnectorConfig.ServiceUrl].TrimEnd('/');
        _tokenUrl = config[EventMeshConnectorConfig.TokenUrl];
        _clientId = config[EventMeshConnectorConfig.ClientId];
        _clientSecret = config[EventMeshConnectorConfig.ClientSecret];
        _queueName = config[EventMeshConnectorConfig.QueueName];

        _pollIntervalMs = int.Parse(config.GetValueOrDefault(EventMeshConnectorConfig.PollIntervalMs,
            EventMeshConnectorConfig.DefaultPollIntervalMs.ToString())!);
        _maxMessages = int.Parse(config.GetValueOrDefault(EventMeshConnectorConfig.MaxMessages,
            EventMeshConnectorConfig.DefaultMaxMessages.ToString())!);
        _ackMode = config.GetValueOrDefault(EventMeshConnectorConfig.AckMode,
            EventMeshConnectorConfig.DefaultAckMode)!;

        _httpClient = new HttpClient();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;
        var records = new List<SourceRecord>();

        try
        {
            await EnsureAccessTokenAsync(cancellationToken);

            // Consume messages from queue
            var url = $"{_serviceUrl}/messagingrest/v1/queues/{Uri.EscapeDataString(_queueName)}/messages/consumption";

            var request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
            request.Headers.Add("x-qos", "1");  // At least once delivery
            request.Content = new StringContent(
                JsonSerializer.Serialize(new { maxMessages = _maxMessages }),
                Encoding.UTF8,
                "application/json");

            var response = await _httpClient!.SendAsync(request, cancellationToken);

            if (response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync(cancellationToken);
                var messages = JsonSerializer.Deserialize<EventMeshMessages>(content);

                if (messages?.Messages != null)
                {
                    foreach (var msg in messages.Messages)
                    {
                        var record = CreateRecord(msg);
                        records.Add(record);

                        if (_ackMode == "manual" && !string.IsNullOrEmpty(msg.MessageId))
                        {
                            _pendingAcks.Add(msg.MessageId);
                        }
                    }
                }
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    private async Task EnsureAccessTokenAsync(CancellationToken ct)
    {
        if (_accessToken != null && DateTime.UtcNow < _tokenExpiry)
        {
            return;
        }

        var request = new HttpRequestMessage(HttpMethod.Post, _tokenUrl);
        request.Content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = _clientId,
            ["client_secret"] = _clientSecret
        });

        var response = await _httpClient!.SendAsync(request, ct);
        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(content);

        _accessToken = doc.RootElement.GetProperty("access_token").GetString();
        var expiresIn = doc.RootElement.GetProperty("expires_in").GetInt32();
        _tokenExpiry = DateTime.UtcNow.AddSeconds(expiresIn - 60);  // 60s buffer
    }

    private SourceRecord CreateRecord(EventMeshMessage msg)
    {
        var msgId = Interlocked.Increment(ref _messageId);

        // Parse CloudEvent if present
        var headers = new Dictionary<string, byte[]>
        {
            ["eventmesh.message_id"] = Encoding.UTF8.GetBytes(msg.MessageId ?? ""),
            ["eventmesh.queue"] = Encoding.UTF8.GetBytes(_queueName)
        };

        // Add CloudEvents attributes
        if (msg.CeSpecVersion != null)
        {
            headers["ce_specversion"] = Encoding.UTF8.GetBytes(msg.CeSpecVersion);
        }
        if (msg.CeType != null)
        {
            headers["ce_type"] = Encoding.UTF8.GetBytes(msg.CeType);
        }
        if (msg.CeSource != null)
        {
            headers["ce_source"] = Encoding.UTF8.GetBytes(msg.CeSource);
        }
        if (msg.CeId != null)
        {
            headers["ce_id"] = Encoding.UTF8.GetBytes(msg.CeId);
        }
        if (msg.CeTime != null)
        {
            headers["ce_time"] = Encoding.UTF8.GetBytes(msg.CeTime);
        }

        var key = msg.CeId ?? msg.MessageId;

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["source"] = "eventmesh",
                ["queue"] = _queueName
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = msgId,
                ["eventmesh_id"] = msg.MessageId ?? ""
            },
            Topic = _topic,
            Key = key != null ? Encoding.UTF8.GetBytes(key) : null,
            Value = msg.Data.HasValue ? Encoding.UTF8.GetBytes(msg.Data.Value.GetRawText()) : [],
            Headers = headers
        };
    }

    public override async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_pendingAcks.Count == 0) return;

        try
        {
            await EnsureAccessTokenAsync(cancellationToken);

            var url = $"{_serviceUrl}/messagingrest/v1/queues/{Uri.EscapeDataString(_queueName)}/messages/acknowledgement";

            var request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _accessToken);
            request.Content = new StringContent(
                JsonSerializer.Serialize(new { messageIds = _pendingAcks }),
                Encoding.UTF8,
                "application/json");

            await _httpClient!.SendAsync(request, cancellationToken);
            _pendingAcks.Clear();
        }
        catch (Exception)
        {
            // Log and continue
        }
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _httpClient?.Dispose();
        }
        base.Dispose(disposing);
    }

    private class EventMeshMessages
    {
        public List<EventMeshMessage>? Messages { get; set; }
    }

    private class EventMeshMessage
    {
        public string? MessageId { get; set; }
        public JsonElement? Data { get; set; }

        // CloudEvents attributes
        public string? CeSpecVersion { get; set; }
        public string? CeType { get; set; }
        public string? CeSource { get; set; }
        public string? CeId { get; set; }
        public string? CeTime { get; set; }
    }
}
