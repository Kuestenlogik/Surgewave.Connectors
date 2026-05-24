using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mattermost;

/// <summary>
/// Source task that polls messages from Mattermost channels via REST API.
/// </summary>
#pragma warning disable CA2213, CA1812
public sealed class MattermostSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _topic = string.Empty;
    private string _serverUrl = string.Empty;
    private HashSet<string>? _channelFilter;
    private bool _includeBotMessages;
    private int _pollIntervalMs;
    private long _messageId;
    private DateTimeOffset _lastPollTime = DateTimeOffset.UtcNow;
    private readonly Dictionary<string, long> _lastPostTimes = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _serverUrl = (config.TryGetValue(MattermostConnectorConfig.ServerUrl, out var su) ? su : MattermostConnectorConfig.DefaultServerUrl).TrimEnd('/');
        var accessToken = config[MattermostConnectorConfig.AccessToken];
        _topic = config[MattermostConnectorConfig.Topic];

        // Parse poll interval
        _pollIntervalMs = config.TryGetValue(MattermostConnectorConfig.PollIntervalMs, out var pi) ? int.Parse(pi) : MattermostConnectorConfig.DefaultPollIntervalMs;

        // Parse channel filter
        if (config.TryGetValue(MattermostConnectorConfig.ChannelIds, out var channels) && !string.IsNullOrWhiteSpace(channels))
        {
            _channelFilter = channels.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        // Include bot messages
        _includeBotMessages = config.TryGetValue(MattermostConnectorConfig.IncludeBotMessages, out var ibm) && ibm == "true";

        // Create HTTP client
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(_serverUrl),
            DefaultRequestHeaders =
            {
                Authorization = new AuthenticationHeaderValue("Bearer", accessToken)
            }
        };
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        // Rate limiting
        var elapsed = (DateTimeOffset.UtcNow - _lastPollTime).TotalMilliseconds;
        if (elapsed < _pollIntervalMs)
        {
            await Task.Delay((int)(_pollIntervalMs - elapsed), cancellationToken);
        }
        _lastPollTime = DateTimeOffset.UtcNow;

        if (_httpClient == null || _channelFilter == null || _channelFilter.Count == 0)
            return records;

        try
        {
            foreach (var channelId in _channelFilter)
            {
                _lastPostTimes.TryGetValue(channelId, out var since);
                if (since == 0)
                    since = DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds();

                var response = await _httpClient.GetAsync(new Uri($"/api/v4/channels/{channelId}/posts?since={since}", UriKind.Relative), cancellationToken);
                if (!response.IsSuccessStatusCode)
                    continue;

                var json = await response.Content.ReadAsStringAsync(cancellationToken);
                var postsResponse = JsonSerializer.Deserialize<PostsResponse>(json, JsonOptions);

                if (postsResponse?.Posts == null)
                    continue;

                long maxTime = since;
                foreach (var (_, post) in postsResponse.Posts)
                {
                    if (post.CreateAt <= since)
                        continue;

                    // Filter bot messages if configured
                    if (!_includeBotMessages && post.Props?.ContainsKey("from_bot") == true)
                        continue;

                    records.Add(CreateSourceRecord(post, channelId));
                    maxTime = Math.Max(maxTime, post.CreateAt);
                }

                _lastPostTimes[channelId] = maxTime;
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }

        return records;
    }

    private SourceRecord CreateSourceRecord(MattermostPost post, string channelId)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["id"] = post.Id,
            ["channel_id"] = post.ChannelId,
            ["user_id"] = post.UserId,
            ["message"] = post.Message,
            ["create_at"] = post.CreateAt,
            ["type"] = post.Type
        };

        var json = JsonSerializer.Serialize(eventData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(post.Id ?? Guid.NewGuid().ToString());

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["channel_id"] = channelId
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["post_id"] = post.Id ?? string.Empty,
                ["create_at"] = post.CreateAt,
                ["message_id"] = Interlocked.Increment(ref _messageId)
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(post.CreateAt),
            Headers = new Dictionary<string, byte[]>
            {
                ["mattermost.channel.id"] = Encoding.UTF8.GetBytes(post.ChannelId ?? string.Empty),
                ["mattermost.user.id"] = Encoding.UTF8.GetBytes(post.UserId ?? string.Empty),
                ["mattermost.post.id"] = Encoding.UTF8.GetBytes(post.Id ?? string.Empty)
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
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    private sealed class PostsResponse
    {
        public Dictionary<string, MattermostPost>? Posts { get; set; }
        public List<string>? Order { get; set; }
    }

    private sealed class MattermostPost
    {
        public string? Id { get; set; }
        public string? ChannelId { get; set; }
        public string? UserId { get; set; }
        public string? Message { get; set; }
        public long CreateAt { get; set; }
        public string? Type { get; set; }
        public Dictionary<string, object>? Props { get; set; }
    }
}
#pragma warning restore CA2213, CA1812
