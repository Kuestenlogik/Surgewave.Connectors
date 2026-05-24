using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RocketChat;

/// <summary>
/// Sink task that posts messages to Rocket.Chat rooms via REST API.
/// </summary>
#pragma warning disable CA2213
public sealed class RocketChatSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _defaultRoomId = string.Empty;
    private string? _roomIdField;
    private string _textField = "text";

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var serverUrl = config.TryGetValue(RocketChatConnectorConfig.ServerUrl, out var su)
            ? su : RocketChatConnectorConfig.DefaultServerUrl;
        serverUrl = serverUrl.TrimEnd('/');

        var userId = config[RocketChatConnectorConfig.UserId];
        var authToken = config[RocketChatConnectorConfig.AuthToken];

        _defaultRoomId = config.TryGetValue(RocketChatConnectorConfig.DefaultRoomId, out var rid) ? rid : string.Empty;
        _roomIdField = config.TryGetValue(RocketChatConnectorConfig.RoomIdField, out var rif) ? rif : null;
        _textField = config.TryGetValue(RocketChatConnectorConfig.TextField, out var tf) ? tf : "text";

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(serverUrl)
        };
        _httpClient.DefaultRequestHeaders.Add("X-Auth-Token", authToken);
        _httpClient.DefaultRequestHeaders.Add("X-User-Id", userId);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_httpClient == null) return;

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                var json = Encoding.UTF8.GetString(record.Value);
                var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json, JsonOptions);
                if (data == null) continue;

                var roomId = _defaultRoomId;
                if (!string.IsNullOrEmpty(_roomIdField) && data.TryGetValue(_roomIdField, out var ridEl))
                {
                    roomId = ridEl.GetString() ?? _defaultRoomId;
                }

                var text = data.TryGetValue(_textField, out var textEl) ? textEl.GetString() : json;

                var payload = new { roomId, text };
                using var content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json");

                using var response = await _httpClient.PostAsync(new Uri("/api/v1/chat.postMessage", UriKind.Relative), content, cancellationToken);
            }
            catch (Exception)
            {
            }
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
        _httpClient?.Dispose();
        _httpClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Stop();
        base.Dispose(disposing);
    }
}
#pragma warning restore CA2213
