using System.Globalization;
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
    private const string PartitionChannelId = "channel_id";
    private const string OffsetCreateAt = "create_at";

    /// <summary>How long a discovered channel list is reused before the account is re-scanned.</summary>
    private static readonly TimeSpan ChannelDiscoveryInterval = TimeSpan.FromMinutes(5);

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
    private HashSet<string> _discoveredChannels = [];
    private DateTimeOffset _channelsDiscoveredAt = DateTimeOffset.MinValue;

    /// <summary>
    /// Creates a task that opens its own HTTP client when it is started.
    /// </summary>
    public MattermostSourceTask()
    {
    }

    /// <summary>
    /// Creates a task that polls through an already-built HTTP client.
    /// </summary>
    internal MattermostSourceTask(HttpClient httpClient) => _httpClient = httpClient;

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
        _httpClient ??= new HttpClient
        {
            BaseAddress = new Uri(_serverUrl)
        };
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
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

        if (_httpClient == null)
            return records;

        try
        {
            foreach (var channelId in await ResolveChannelsAsync(cancellationToken))
            {
                var since = GetSince(channelId);

                using var response = await _httpClient.GetAsync(
                    new Uri($"/api/v4/channels/{channelId}/posts?since={since.ToString(CultureInfo.InvariantCulture)}", UriKind.Relative),
                    cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    Context?.RaiseError?.Invoke(new HttpRequestException(
                        $"Mattermost post fetch for channel '{channelId}' failed with status {(int)response.StatusCode} ({response.StatusCode})"));
                    continue;
                }

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

    /// <summary>
    /// Resolves the channels to poll. An empty 'mattermost.channel.ids' means "all channels the
    /// token can see", so the account's teams are scanned instead of polling nothing at all.
    /// </summary>
    private async Task<HashSet<string>> ResolveChannelsAsync(CancellationToken cancellationToken)
    {
        var configured = _channelFilter;
        if (configured is { Count: > 0 })
            return configured;

        if (_discoveredChannels.Count > 0 && DateTimeOffset.UtcNow - _channelsDiscoveredAt < ChannelDiscoveryInterval)
            return _discoveredChannels;

        using var teamsResponse = await _httpClient!.GetAsync(new Uri("/api/v4/users/me/teams", UriKind.Relative), cancellationToken);
        if (!teamsResponse.IsSuccessStatusCode)
        {
            Context?.RaiseError?.Invoke(new HttpRequestException(
                $"Mattermost team listing failed with status {(int)teamsResponse.StatusCode} ({teamsResponse.StatusCode})"));
            return _discoveredChannels;
        }

        var teamsJson = await teamsResponse.Content.ReadAsStringAsync(cancellationToken);
        var teams = JsonSerializer.Deserialize<List<MattermostTeam>>(teamsJson, JsonOptions) ?? [];

        var discovered = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var team in teams)
        {
            if (string.IsNullOrEmpty(team.Id))
                continue;

            using var channelsResponse = await _httpClient.GetAsync(
                new Uri($"/api/v4/users/me/teams/{team.Id}/channels", UriKind.Relative), cancellationToken);
            if (!channelsResponse.IsSuccessStatusCode)
            {
                Context?.RaiseError?.Invoke(new HttpRequestException(
                    $"Mattermost channel listing for team '{team.Id}' failed with status {(int)channelsResponse.StatusCode} ({channelsResponse.StatusCode})"));
                continue;
            }

            var channelsJson = await channelsResponse.Content.ReadAsStringAsync(cancellationToken);
            var channels = JsonSerializer.Deserialize<List<MattermostChannel>>(channelsJson, JsonOptions) ?? [];
            foreach (var channel in channels)
            {
                if (!string.IsNullOrEmpty(channel.Id))
                    discovered.Add(channel.Id);
            }
        }

        _discoveredChannels = discovered;
        _channelsDiscoveredAt = DateTimeOffset.UtcNow;
        return discovered;
    }

    /// <summary>
    /// Returns the cursor to poll a channel from: the live one, else the one persisted with the
    /// last emitted post, else a short look-back. Without the restore a restart longer than the
    /// look-back would silently skip every post written while the task was down.
    /// </summary>
    private long GetSince(string channelId)
    {
        if (_lastPostTimes.TryGetValue(channelId, out var since) && since > 0)
            return since;

        var storedOffset = Context?.OffsetStorageReader?.Offset(
            new Dictionary<string, object> { [PartitionChannelId] = channelId });

        if (storedOffset != null &&
            storedOffset.TryGetValue(OffsetCreateAt, out var createAt) && createAt != null &&
            long.TryParse(createAt.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var stored) &&
            stored > 0)
        {
            since = stored;
        }
        else
        {
            since = DateTimeOffset.UtcNow.AddMinutes(-5).ToUnixTimeMilliseconds();
        }

        _lastPostTimes[channelId] = since;
        return since;
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
                [PartitionChannelId] = channelId
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["post_id"] = post.Id ?? string.Empty,
                [OffsetCreateAt] = post.CreateAt,
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

    private sealed class MattermostTeam
    {
        public string? Id { get; set; }
    }

    private sealed class MattermostChannel
    {
        public string? Id { get; set; }
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
