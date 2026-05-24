using System.Text;
using System.Text.Json;
using Reddit;
using Reddit.Controllers;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Reddit;

/// <summary>
/// Source task that polls subreddits for new posts and comments.
/// </summary>
public class RedditSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false
    };

    private IDictionary<string, string> _config = new Dictionary<string, string>();
    private string _topic = string.Empty;

    // Reddit client
    private RedditClient? _redditClient;

    // Settings
    private List<string> _subreddits = [];
    private int _pollIntervalMs = RedditConnectorConfig.DefaultPollIntervalMs;
    private string _contentType = RedditConnectorConfig.DefaultContentType;
    private string _sortBy = RedditConnectorConfig.DefaultSortBy;
    private string _timePeriod = RedditConnectorConfig.DefaultTimePeriod;
    private int _maxPostsPerPoll = RedditConnectorConfig.DefaultMaxPostsPerPoll;
    private bool _includeComments = RedditConnectorConfig.DefaultIncludeComments;
    private int _maxCommentsPerPost = RedditConnectorConfig.DefaultMaxCommentsPerPost;
    private HashSet<string>? _flairFilter;
    private string _nsfwFilter = RedditConnectorConfig.DefaultNsfwFilter;

    // State tracking
    private readonly Dictionary<string, HashSet<string>> _seenPostIds = new();
    private readonly Dictionary<string, HashSet<string>> _seenCommentIds = new();
    private DateTime _lastPollTime = DateTime.MinValue;
    private string? _lastPostId;
    private string? _lastCommentId;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;
        _topic = config[RedditConnectorConfig.TopicConfig];

        var clientId = config[RedditConnectorConfig.ClientIdConfig];
        var clientSecret = config.GetValueOrDefault(RedditConnectorConfig.ClientSecretConfig);
        var userAgent = config.GetValueOrDefault(RedditConnectorConfig.UserAgentConfig);

        // Parse subreddits
        if (config.TryGetValue(RedditConnectorConfig.SubredditsConfig, out var subredditsStr) &&
            !string.IsNullOrWhiteSpace(subredditsStr))
        {
            _subreddits = subredditsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToList();
        }

        // Parse poll interval
        if (config.TryGetValue(RedditConnectorConfig.PollIntervalMsConfig, out var pollIntervalStr) &&
            int.TryParse(pollIntervalStr, out var pollInterval))
        {
            _pollIntervalMs = pollInterval;
        }

        // Parse content type
        if (config.TryGetValue(RedditConnectorConfig.ContentTypeConfig, out var contentType) &&
            !string.IsNullOrWhiteSpace(contentType))
        {
            _contentType = contentType.ToLowerInvariant();
        }

        // Parse sort by
        if (config.TryGetValue(RedditConnectorConfig.SortByConfig, out var sortBy) &&
            !string.IsNullOrWhiteSpace(sortBy))
        {
            _sortBy = sortBy.ToLowerInvariant();
        }

        // Parse time period
        if (config.TryGetValue(RedditConnectorConfig.TimePeriodConfig, out var timePeriod) &&
            !string.IsNullOrWhiteSpace(timePeriod))
        {
            _timePeriod = timePeriod.ToLowerInvariant();
        }

        // Parse max posts
        if (config.TryGetValue(RedditConnectorConfig.MaxPostsPerPollConfig, out var maxPostsStr) &&
            int.TryParse(maxPostsStr, out var maxPosts))
        {
            _maxPostsPerPoll = maxPosts;
        }

        // Parse include comments
        if (config.TryGetValue(RedditConnectorConfig.IncludeCommentsConfig, out var includeCommentsStr) &&
            bool.TryParse(includeCommentsStr, out var includeComments))
        {
            _includeComments = includeComments;
        }

        // Parse max comments
        if (config.TryGetValue(RedditConnectorConfig.MaxCommentsPerPostConfig, out var maxCommentsStr) &&
            int.TryParse(maxCommentsStr, out var maxComments))
        {
            _maxCommentsPerPost = maxComments;
        }

        // Parse flair filter
        if (config.TryGetValue(RedditConnectorConfig.FlairFilterConfig, out var flairFilter) &&
            !string.IsNullOrWhiteSpace(flairFilter))
        {
            _flairFilter = flairFilter.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        // Parse NSFW filter
        if (config.TryGetValue(RedditConnectorConfig.NsfwFilterConfig, out var nsfwFilter) &&
            !string.IsNullOrWhiteSpace(nsfwFilter))
        {
            _nsfwFilter = nsfwFilter.ToLowerInvariant();
        }

        // Initialize seen IDs tracking for each subreddit
        foreach (var sub in _subreddits)
        {
            _seenPostIds[sub] = [];
            _seenCommentIds[sub] = [];
        }

        // Initialize Reddit client - Reddit.NET uses refresh token for authentication
        // For read-only operations, we can use just the app ID
        _redditClient = new RedditClient(
            appId: clientId,
            appSecret: clientSecret
        );
    }

    public override void Stop()
    {
        _redditClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _redditClient = null;
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        if (_redditClient == null || _subreddits.Count == 0)
            return records;

        // Respect poll interval
        var timeSinceLastPoll = DateTime.UtcNow - _lastPollTime;
        if (timeSinceLastPoll.TotalMilliseconds < _pollIntervalMs)
        {
            var waitTime = TimeSpan.FromMilliseconds(_pollIntervalMs - timeSinceLastPoll.TotalMilliseconds);
            await Task.Delay(waitTime, cancellationToken);
        }

        _lastPollTime = DateTime.UtcNow;

        foreach (var subredditName in _subreddits)
        {
            try
            {
                var subreddit = _redditClient.Subreddit(subredditName).About();

                // Fetch posts if needed
                if (_contentType is RedditConnectorConfig.ContentTypePosts or RedditConnectorConfig.ContentTypeBoth)
                {
                    var posts = await Task.Run(() => FetchPosts(subreddit), cancellationToken);
                    foreach (var post in posts)
                    {
                        if (PassesFilter(post) && !_seenPostIds[subredditName].Contains(post.Fullname))
                        {
                            _seenPostIds[subredditName].Add(post.Fullname);
                            var record = CreatePostRecord(post, subredditName);
                            records.Add(record);
                            _lastPostId = post.Fullname;

                            // Fetch comments for this post if configured
                            if (_includeComments)
                            {
                                var comments = await Task.Run(() => FetchComments(post), cancellationToken);
                                foreach (var comment in comments.Take(_maxCommentsPerPost))
                                {
                                    if (!_seenCommentIds[subredditName].Contains(comment.Fullname))
                                    {
                                        _seenCommentIds[subredditName].Add(comment.Fullname);
                                        var commentRecord = CreateCommentRecord(comment, post, subredditName);
                                        records.Add(commentRecord);
                                        _lastCommentId = comment.Fullname;
                                    }
                                }
                            }
                        }
                    }
                }

                // Fetch comments directly if needed
                if (_contentType is RedditConnectorConfig.ContentTypeComments)
                {
                    var comments = await Task.Run(() => FetchSubredditComments(subreddit), cancellationToken);
                    foreach (var comment in comments)
                    {
                        if (!_seenCommentIds[subredditName].Contains(comment.Fullname))
                        {
                            _seenCommentIds[subredditName].Add(comment.Fullname);
                            var record = CreateCommentRecord(comment, null, subredditName);
                            records.Add(record);
                            _lastCommentId = comment.Fullname;
                        }
                    }
                }

                // Limit seen IDs to prevent memory issues
                if (_seenPostIds[subredditName].Count > 10000)
                {
                    _seenPostIds[subredditName] = _seenPostIds[subredditName].TakeLast(5000).ToHashSet();
                }
                if (_seenCommentIds[subredditName].Count > 10000)
                {
                    _seenCommentIds[subredditName] = _seenCommentIds[subredditName].TakeLast(5000).ToHashSet();
                }
            }
            catch (Exception ex)
            {
                // Log error but continue with other subreddits
                Console.Error.WriteLine($"Error polling subreddit {subredditName}: {ex.Message}");
            }
        }

        return records;
    }

    public override Task CommitAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets the current offset for checkpoint/resume purposes.
    /// </summary>
    public IDictionary<string, object>? CurrentOffset =>
        _lastPostId == null && _lastCommentId == null
            ? null
            : new Dictionary<string, object>
            {
                [RedditConnectorConfig.OffsetPostId] = _lastPostId ?? string.Empty,
                [RedditConnectorConfig.OffsetCommentId] = _lastCommentId ?? string.Empty,
                [RedditConnectorConfig.OffsetTimestamp] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            };

    private List<Post> FetchPosts(Subreddit subreddit)
    {
        return _sortBy switch
        {
            RedditConnectorConfig.SortHot => subreddit.Posts.Hot.Take(_maxPostsPerPoll).ToList(),
            RedditConnectorConfig.SortNew => subreddit.Posts.New.Take(_maxPostsPerPoll).ToList(),
            RedditConnectorConfig.SortTop => subreddit.Posts.Top.Take(_maxPostsPerPoll).ToList(),
            RedditConnectorConfig.SortRising => subreddit.Posts.Rising.Take(_maxPostsPerPoll).ToList(),
            RedditConnectorConfig.SortControversial => subreddit.Posts.Controversial.Take(_maxPostsPerPoll).ToList(),
            _ => subreddit.Posts.New.Take(_maxPostsPerPoll).ToList()
        };
    }

    private List<Comment> FetchComments(Post post)
    {
        // Get top-level comments
        var comments = new List<Comment>();
        var postComments = post.Comments.GetComments(limit: _maxCommentsPerPost);
        if (postComments != null)
        {
            comments.AddRange(postComments);
        }
        return comments;
    }

    private List<Comment> FetchSubredditComments(Subreddit subreddit)
    {
        // Get new comments from subreddit
        var comments = subreddit.Comments.GetNew(limit: _maxPostsPerPoll);
        return comments ?? [];
    }

    private bool PassesFilter(Post post)
    {
        // NSFW filter
        if (post.NSFW)
        {
            if (_nsfwFilter == RedditConnectorConfig.NsfwExclude)
                return false;
        }
        else
        {
            if (_nsfwFilter == RedditConnectorConfig.NsfwOnly)
                return false;
        }

        // Flair filter
        if (_flairFilter != null && !string.IsNullOrEmpty(post.Listing.LinkFlairText))
        {
            if (!_flairFilter.Contains(post.Listing.LinkFlairText))
                return false;
        }

        return true;
    }

    private SourceRecord CreatePostRecord(Post post, string subreddit)
    {
        var postData = new Dictionary<string, object?>
        {
            ["id"] = post.Id,
            ["fullname"] = post.Fullname,
            ["type"] = "post",
            ["subreddit"] = subreddit,
            ["author"] = post.Author,
            ["title"] = post.Title,
            ["selftext"] = post.Listing.SelfText,
            ["url"] = post.Listing.URL,
            ["permalink"] = post.Permalink,
            ["score"] = post.Score,
            ["upvote_ratio"] = post.UpvoteRatio,
            ["num_comments"] = post.Listing.NumComments,
            ["created_utc"] = post.Created,
            ["is_self"] = post.Listing.IsSelf,
            ["is_video"] = post.Listing.IsVideo,
            ["nsfw"] = post.NSFW,
            ["spoiler"] = post.Listing.Spoiler,
            ["flair"] = post.Listing.LinkFlairText,
            ["thumbnail"] = post.Listing.Thumbnail,
            ["domain"] = post.Listing.Domain
        };

        var json = JsonSerializer.Serialize(postData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(post.Fullname);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["subreddit"] = subreddit
            },
            SourceOffset = new Dictionary<string, object>
            {
                [RedditConnectorConfig.OffsetPostId] = post.Fullname,
                [RedditConnectorConfig.OffsetSubreddit] = subreddit,
                [RedditConnectorConfig.OffsetTimestamp] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = post.Created,
            Headers = new Dictionary<string, byte[]>
            {
                ["reddit.type"] = Encoding.UTF8.GetBytes("post"),
                ["reddit.subreddit"] = Encoding.UTF8.GetBytes(subreddit),
                ["reddit.author"] = Encoding.UTF8.GetBytes(post.Author ?? string.Empty),
                ["reddit.id"] = Encoding.UTF8.GetBytes(post.Id)
            }
        };
    }

    private SourceRecord CreateCommentRecord(Comment comment, Post? post, string subreddit)
    {
        var commentData = new Dictionary<string, object?>
        {
            ["id"] = comment.Id,
            ["fullname"] = comment.Fullname,
            ["type"] = "comment",
            ["subreddit"] = subreddit,
            ["author"] = comment.Author,
            ["body"] = comment.Body,
            ["score"] = comment.Score,
            ["created_utc"] = comment.Created,
            ["parent_id"] = comment.ParentFullname,
            ["post_id"] = post?.Fullname,
            ["post_title"] = post?.Title,
            ["permalink"] = comment.Permalink,
            ["is_submitter"] = comment.IsSubmitter,
            ["depth"] = comment.Depth
        };

        var json = JsonSerializer.Serialize(commentData, JsonOptions);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(comment.Fullname);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["subreddit"] = subreddit
            },
            SourceOffset = new Dictionary<string, object>
            {
                [RedditConnectorConfig.OffsetCommentId] = comment.Fullname,
                [RedditConnectorConfig.OffsetSubreddit] = subreddit,
                [RedditConnectorConfig.OffsetTimestamp] = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = comment.Created,
            Headers = new Dictionary<string, byte[]>
            {
                ["reddit.type"] = Encoding.UTF8.GetBytes("comment"),
                ["reddit.subreddit"] = Encoding.UTF8.GetBytes(subreddit),
                ["reddit.author"] = Encoding.UTF8.GetBytes(comment.Author ?? string.Empty),
                ["reddit.id"] = Encoding.UTF8.GetBytes(comment.Id)
            }
        };
    }
}
