using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.RocketChat;

/// <summary>
/// Source task that polls messages from Rocket.Chat rooms via REST API.
/// </summary>
#pragma warning disable CA2213
public sealed class RocketChatSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _topic = string.Empty;
    private HashSet<string>? _roomIds;
    private bool _includeBotMessages;
    private int _pollIntervalMs;
    private long _messageId;
    private DateTimeOffset _lastPollTime = DateTimeOffset.UtcNow;
    private readonly Dictionary<string, DateTimeOffset> _lastMessageTimes = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var serverUrl = config.TryGetValue(RocketChatConnectorConfig.ServerUrl, out var su)
            ? su : RocketChatConnectorConfig.DefaultServerUrl;
        serverUrl = serverUrl.TrimEnd('/');

        var userId = config[RocketChatConnectorConfig.UserId];
        var authToken = config[RocketChatConnectorConfig.AuthToken];
        _topic = config[RocketChatConnectorConfig.Topic];

        _pollIntervalMs = config.TryGetValue(RocketChatConnectorConfig.PollIntervalMs, out var pi)
            ? int.Parse(pi) : RocketChatConnectorConfig.DefaultPollIntervalMs;

        if (config.TryGetValue(RocketChatConnectorConfig.RoomIds, out var rooms) && !string.IsNullOrWhiteSpace(rooms))
        {
            _roomIds = rooms.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        _includeBotMessages = config.TryGetValue(RocketChatConnectorConfig.IncludeBotMessages, out var ibm) && ibm == "true";

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(serverUrl)
        };
        _httpClient.DefaultRequestHeaders.Add("X-Auth-Token", authToken);
        _httpClient.DefaultRequestHeaders.Add("X-User-Id", userId);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        var elapsed = (DateTimeOffset.UtcNow - _lastPollTime).TotalMilliseconds;
        if (elapsed < _pollIntervalMs)
        {
            await Task.Delay((int)(_pollIntervalMs - elapsed), cancellationToken);
        }
        _lastPollTime = DateTimeOffset.UtcNow;

        if (_httpClient == null || _roomIds == null || _roomIds.Count == 0)
            return records;

        try
        {
            foreach (var roomId in _roomIds)
            {
                _lastMessageTimes.TryGetValue(roomId, out var since);
                if (since == default)
                    since = DateTimeOffset.UtcNow.AddMinutes(-5);

                var response = await _httpClient.GetAsync(
                    new Uri($"/api/v1/channels.history?roomId={roomId}&oldest={since:O}&count=100", UriKind.Relative),
                    cancellationToken);

                if (!response.IsSuccessStatusCode)
                    continue;

                var json = await response.Content.ReadAsStringAsync(cancellationToken);
                var historyResponse = JsonSerializer.Deserialize<HistoryResponse>(json, JsonOptions);

                if (historyResponse?.Messages == null)
                    continue;

                var maxTime = since;
                foreach (var message in historyResponse.Messages.OrderBy(m => m.Ts))
                {
                    if (message.Ts <= since)
                        continue;

                    if (!_includeBotMessages && message.Bot != null)
                        continue;

                    records.Add(CreateSourceRecord(message, roomId));
                    if (message.Ts > maxTime)
                        maxTime = message.Ts;
                }

                _lastMessageTimes[roomId] = maxTime;
            }
        }
        catch (OperationCanceledException)
        {
        }

        return records;
    }

    private SourceRecord CreateSourceRecord(RocketChatMessage message, string roomId)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["_id"] = message.Id,
            ["rid"] = message.Rid,
            ["msg"] = message.Msg,
            ["ts"] = message.Ts,
            ["u"] = message.User != null ? new { id = message.User.Id, username = message.User.Username, name = message.User.Name } : null
        };

        var json = JsonSerializer.Serialize(eventData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(message.Id ?? Guid.NewGuid().ToString());

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["room_id"] = roomId
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["message_id"] = message.Id ?? string.Empty,
                ["ts"] = message.Ts.ToUnixTimeMilliseconds(),
                ["offset"] = Interlocked.Increment(ref _messageId)
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = message.Ts,
            Headers = new Dictionary<string, byte[]>
            {
                ["rocketchat.room.id"] = Encoding.UTF8.GetBytes(roomId),
                ["rocketchat.user.id"] = Encoding.UTF8.GetBytes(message.User?.Id ?? string.Empty),
                ["rocketchat.message.id"] = Encoding.UTF8.GetBytes(message.Id ?? string.Empty)
            }
        };
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

#pragma warning disable CA1812
    private sealed class HistoryResponse
    {
        public List<RocketChatMessage>? Messages { get; set; }
        public bool Success { get; set; }
    }

    private sealed class RocketChatMessage
    {
        public string? Id { get; set; }
        public string? Rid { get; set; }
        public string? Msg { get; set; }
        public DateTimeOffset Ts { get; set; }
        public RocketChatUser? User { get; set; }
        public object? Bot { get; set; }
    }

    private sealed class RocketChatUser
    {
        public string? Id { get; set; }
        public string? Username { get; set; }
        public string? Name { get; set; }
    }
#pragma warning restore CA1812
}
#pragma warning restore CA2213
