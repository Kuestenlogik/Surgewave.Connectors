using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Twitter;

/// <summary>
/// Source task that polls tweets from Twitter/X via API v2 using HttpClient.
/// </summary>
#pragma warning disable CA2213
public sealed class TwitterSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _topic = string.Empty;
    private string? _searchQuery;
    private HashSet<string>? _userIds;
    private int _pollIntervalMs;
    private int _maxResults;
    private bool _includeRetweets;
    private bool _includeReplies;
    private long _messageId;
    private DateTimeOffset _lastPollTime = DateTimeOffset.UtcNow;
    private string? _sinceId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[TwitterConnectorConfig.Topic];

        var bearerToken = config[TwitterConnectorConfig.BearerToken];

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri("https://api.twitter.com/2/")
        };
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", bearerToken);

        _searchQuery = config.TryGetValue(TwitterConnectorConfig.SearchQuery, out var sq) ? sq : null;

        if (config.TryGetValue(TwitterConnectorConfig.UserIds, out var uids) && !string.IsNullOrWhiteSpace(uids))
        {
            _userIds = uids.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        _pollIntervalMs = config.TryGetValue(TwitterConnectorConfig.PollIntervalMs, out var pi)
            ? int.Parse(pi) : TwitterConnectorConfig.DefaultPollIntervalMs;
        _maxResults = config.TryGetValue(TwitterConnectorConfig.MaxResults, out var mr)
            ? int.Parse(mr) : TwitterConnectorConfig.DefaultMaxResults;
        _includeRetweets = !config.TryGetValue(TwitterConnectorConfig.IncludeRetweets, out var ir) || ir != "false";
        _includeReplies = !config.TryGetValue(TwitterConnectorConfig.IncludeReplies, out var irp) || irp != "false";
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

        if (_httpClient == null) return records;

        try
        {
            if (!string.IsNullOrEmpty(_searchQuery))
            {
                var url = $"tweets/search/recent?query={Uri.EscapeDataString(_searchQuery)}&max_results={_maxResults}&tweet.fields=created_at,author_id,in_reply_to_user_id,public_metrics";
                if (!string.IsNullOrEmpty(_sinceId))
                {
                    url += $"&since_id={_sinceId}";
                }

                var response = await _httpClient.GetAsync(new Uri(url, UriKind.Relative), cancellationToken);
                if (!response.IsSuccessStatusCode) return records;

                var json = await response.Content.ReadAsStringAsync(cancellationToken);
                var searchResult = JsonSerializer.Deserialize<TwitterSearchResponse>(json, JsonOptions);

                if (searchResult?.Data != null)
                {
                    foreach (var tweet in searchResult.Data)
                    {
                        if (!_includeRetweets && tweet.Text?.StartsWith("RT @", StringComparison.Ordinal) == true)
                            continue;
                        if (!_includeReplies && !string.IsNullOrEmpty(tweet.InReplyToUserId))
                            continue;

                        records.Add(CreateSourceRecord(tweet));
                        if (_sinceId == null || string.Compare(tweet.Id, _sinceId, StringComparison.Ordinal) > 0)
                        {
                            _sinceId = tweet.Id;
                        }
                    }
                }
            }
            else if (_userIds != null && _userIds.Count > 0)
            {
                foreach (var userId in _userIds)
                {
                    var url = $"users/{userId}/tweets?max_results={_maxResults}&tweet.fields=created_at,author_id,in_reply_to_user_id,public_metrics";

                    var response = await _httpClient.GetAsync(new Uri(url, UriKind.Relative), cancellationToken);
                    if (!response.IsSuccessStatusCode) continue;

                    var json = await response.Content.ReadAsStringAsync(cancellationToken);
                    var userTweets = JsonSerializer.Deserialize<TwitterSearchResponse>(json, JsonOptions);

                    if (userTweets?.Data != null)
                    {
                        foreach (var tweet in userTweets.Data)
                        {
                            if (!_includeRetweets && tweet.Text?.StartsWith("RT @", StringComparison.Ordinal) == true)
                                continue;
                            if (!_includeReplies && !string.IsNullOrEmpty(tweet.InReplyToUserId))
                                continue;

                            records.Add(CreateSourceRecord(tweet));
                        }
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception)
        {
            // Rate limit or API error
        }

        return records;
    }

    private SourceRecord CreateSourceRecord(TweetData tweet)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["id"] = tweet.Id,
            ["text"] = tweet.Text,
            ["created_at"] = tweet.CreatedAt,
            ["author_id"] = tweet.AuthorId,
            ["in_reply_to_user_id"] = tweet.InReplyToUserId,
            ["retweet_count"] = tweet.PublicMetrics?.RetweetCount,
            ["like_count"] = tweet.PublicMetrics?.LikeCount,
            ["reply_count"] = tweet.PublicMetrics?.ReplyCount
        };

        var json = JsonSerializer.Serialize(eventData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(tweet.Id ?? Guid.NewGuid().ToString());

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["author_id"] = tweet.AuthorId ?? "unknown"
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["tweet_id"] = tweet.Id ?? string.Empty,
                ["offset"] = Interlocked.Increment(ref _messageId)
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = tweet.CreatedAt ?? DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                ["twitter.tweet.id"] = Encoding.UTF8.GetBytes(tweet.Id ?? string.Empty),
                ["twitter.author.id"] = Encoding.UTF8.GetBytes(tweet.AuthorId ?? string.Empty)
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
    private sealed class TwitterSearchResponse
    {
        public List<TweetData>? Data { get; set; }
        public TwitterMeta? Meta { get; set; }
    }

    private sealed class TweetData
    {
        public string? Id { get; set; }
        public string? Text { get; set; }
        public string? AuthorId { get; set; }
        public DateTimeOffset? CreatedAt { get; set; }
        public string? InReplyToUserId { get; set; }
        public TweetPublicMetrics? PublicMetrics { get; set; }
    }

    private sealed class TweetPublicMetrics
    {
        public int RetweetCount { get; set; }
        public int LikeCount { get; set; }
        public int ReplyCount { get; set; }
    }

    private sealed class TwitterMeta
    {
        public string? NewestId { get; set; }
        public string? OldestId { get; set; }
        public int ResultCount { get; set; }
    }
#pragma warning restore CA1812
}
#pragma warning restore CA2213
