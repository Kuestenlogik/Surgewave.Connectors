using System.Text;
using System.Text.Json;
using Reddit;
using Reddit.Controllers;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Reddit;

/// <summary>
/// Sink task that posts content and comments to subreddits.
/// </summary>
public class RedditSinkTask : SinkTask
{
    private IDictionary<string, string> _config = new Dictionary<string, string>();

    // Reddit client
    private RedditClient? _redditClient;

    // Settings
    private string? _defaultSubreddit;
    private string? _subredditField;
    private string? _titleField;
    private string? _textField;
    private string? _urlField;
    private string? _flairField;
    private string? _nsfwField;
    private string? _spoilerField;
    private string _postType = RedditConnectorConfig.DefaultPostType;
    private string? _parentIdField;
    private bool _replyToComments = RedditConnectorConfig.DefaultReplyToComments;
    private int _retryCount = RedditConnectorConfig.DefaultRetryCount;
    private int _retryDelayMs = RedditConnectorConfig.DefaultRetryDelayMs;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        var clientId = config[RedditConnectorConfig.ClientIdConfig];
        var clientSecret = config.GetValueOrDefault(RedditConnectorConfig.ClientSecretConfig);

        // Subreddit settings
        config.TryGetValue(RedditConnectorConfig.DefaultSubredditConfig, out _defaultSubreddit);
        config.TryGetValue(RedditConnectorConfig.SubredditFieldConfig, out _subredditField);

        // Post content fields
        config.TryGetValue(RedditConnectorConfig.TitleFieldConfig, out _titleField);
        config.TryGetValue(RedditConnectorConfig.TextFieldConfig, out _textField);
        config.TryGetValue(RedditConnectorConfig.UrlFieldConfig, out _urlField);
        config.TryGetValue(RedditConnectorConfig.FlairFieldConfig, out _flairField);
        config.TryGetValue(RedditConnectorConfig.NsfwFieldConfig, out _nsfwField);
        config.TryGetValue(RedditConnectorConfig.SpoilerFieldConfig, out _spoilerField);

        // Post type
        if (config.TryGetValue(RedditConnectorConfig.PostTypeConfig, out var postType) &&
            !string.IsNullOrWhiteSpace(postType))
        {
            _postType = postType.ToLowerInvariant();
        }

        // Comment settings
        config.TryGetValue(RedditConnectorConfig.ParentIdFieldConfig, out _parentIdField);
        if (config.TryGetValue(RedditConnectorConfig.ReplyToCommentsConfig, out var replyStr) &&
            bool.TryParse(replyStr, out var reply))
        {
            _replyToComments = reply;
        }

        // Behavior settings
        if (config.TryGetValue(RedditConnectorConfig.RetryCountConfig, out var retryCountStr) &&
            int.TryParse(retryCountStr, out var retryCount))
        {
            _retryCount = retryCount;
        }

        if (config.TryGetValue(RedditConnectorConfig.RetryDelayMsConfig, out var retryDelayStr) &&
            int.TryParse(retryDelayStr, out var retryDelay))
        {
            _retryDelayMs = retryDelay;
        }

        // Initialize Reddit client - Reddit.NET requires OAuth authentication for posting
        // The refresh token should be obtained via OAuth flow
        var refreshToken = config.GetValueOrDefault(RedditConnectorConfig.PasswordConfig);

        _redditClient = new RedditClient(
            appId: clientId,
            appSecret: clientSecret,
            refreshToken: refreshToken
        );
    }

    public override void Stop()
    {
        _redditClient = null;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_redditClient == null)
            throw new InvalidOperationException("Reddit client not initialized");

        foreach (var record in records)
        {
            await ProcessRecordWithRetryAsync(record, cancellationToken);
        }
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    private async Task ProcessRecordWithRetryAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var lastException = default(Exception);

        for (var attempt = 0; attempt <= _retryCount; attempt++)
        {
            try
            {
                if (_replyToComments)
                {
                    await Task.Run(() => PostComment(record), cancellationToken);
                }
                else
                {
                    await Task.Run(() => PostSubmission(record), cancellationToken);
                }
                return;
            }
            catch (Exception ex) when (attempt < _retryCount)
            {
                // Check for rate limiting
                if (ex.Message.Contains("rate", StringComparison.OrdinalIgnoreCase) ||
                    ex.Message.Contains("RATELIMIT", StringComparison.OrdinalIgnoreCase))
                {
                    // Wait longer for rate limits
                    await Task.Delay(_retryDelayMs * (attempt + 1) * 2, cancellationToken);
                }
                else
                {
                    await Task.Delay(_retryDelayMs * (attempt + 1), cancellationToken);
                }
                lastException = ex;
            }
        }

        throw lastException ?? new InvalidOperationException("Unknown error during Reddit API call");
    }

    private void PostSubmission(SinkRecord record)
    {
        if (_redditClient == null)
            return;

        var (subreddit, json) = ParseRecord(record);
        if (string.IsNullOrEmpty(subreddit))
            throw new InvalidOperationException("No subreddit specified for post");

        var title = GetFieldValue(json, _titleField) ?? GetDefaultTitle(record);
        if (string.IsNullOrEmpty(title))
            throw new InvalidOperationException("No title specified for post");

        var sub = _redditClient.Subreddit(subreddit).About();

        // Get optional fields
        var nsfw = GetBooleanField(json, _nsfwField) ?? false;
        var flairId = GetFieldValue(json, _flairField);

        Post? result = null;

        switch (_postType)
        {
            case RedditConnectorConfig.PostTypeLink:
                var url = GetFieldValue(json, _urlField);
                if (string.IsNullOrEmpty(url))
                    throw new InvalidOperationException("No URL specified for link post");

                result = sub.LinkPost(title, url).Submit();
                break;

            case RedditConnectorConfig.PostTypeSelf:
            default:
                var text = GetFieldValue(json, _textField) ?? GetPlainTextValue(record);
                result = sub.SelfPost(title, text ?? string.Empty).Submit();
                break;
        }

        // Apply additional settings if available
        if (result != null)
        {
            if (nsfw)
            {
                result.MarkNSFW();
            }

            if (!string.IsNullOrEmpty(flairId))
            {
                result.SetFlair(flairId, string.Empty);
            }
        }
    }

    private void PostComment(SinkRecord record)
    {
        if (_redditClient == null)
            return;

        var (_, json) = ParseRecord(record);

        // Get parent ID
        var parentId = GetFieldValue(json, _parentIdField);
        if (string.IsNullOrEmpty(parentId))
            throw new InvalidOperationException("No parent ID specified for comment");

        // Get comment text
        var text = GetFieldValue(json, _textField) ?? GetPlainTextValue(record);
        if (string.IsNullOrEmpty(text))
            throw new InvalidOperationException("No text specified for comment");

        // Determine if parent is a post or comment
        if (parentId.StartsWith("t3_", StringComparison.Ordinal))
        {
            // Parent is a post
            var postId = parentId[3..];
            var post = _redditClient.Post(postId).About();
            post.Reply(text);
        }
        else if (parentId.StartsWith("t1_", StringComparison.Ordinal))
        {
            // Parent is a comment
            var commentId = parentId[3..];
            var comment = _redditClient.Comment(commentId).About();
            comment.Reply(text);
        }
        else
        {
            // Try as raw ID - assume post
            var post = _redditClient.Post(parentId).About();
            post.Reply(text);
        }
    }

    private (string? subreddit, JsonElement? json) ParseRecord(SinkRecord record)
    {
        JsonElement? json = null;
        string? subreddit = _defaultSubreddit;

        // Try to parse value as JSON
        if (record.Value != null && record.Value.Length > 0)
        {
            try
            {
                var valueStr = Encoding.UTF8.GetString(record.Value);
                using var doc = JsonDocument.Parse(valueStr);
                json = doc.RootElement.Clone();

                // Extract subreddit from JSON if configured
                if (!string.IsNullOrEmpty(_subredditField) && json.Value.TryGetProperty(_subredditField, out var subredditElement))
                {
                    subreddit = subredditElement.GetString() ?? subreddit;
                }
            }
            catch (JsonException)
            {
                // Not JSON, use as plain text
            }
        }

        return (subreddit, json);
    }

    private static string? GetFieldValue(JsonElement? json, string? fieldName)
    {
        if (json == null || string.IsNullOrEmpty(fieldName))
            return null;

        if (json.Value.TryGetProperty(fieldName, out var element))
        {
            return element.ValueKind switch
            {
                JsonValueKind.String => element.GetString(),
                JsonValueKind.Number => element.GetRawText(),
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                _ => element.GetRawText()
            };
        }

        return null;
    }

    private static bool? GetBooleanField(JsonElement? json, string? fieldName)
    {
        if (json == null || string.IsNullOrEmpty(fieldName))
            return null;

        if (json.Value.TryGetProperty(fieldName, out var element))
        {
            return element.ValueKind switch
            {
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.String => bool.TryParse(element.GetString(), out var b) ? b : null,
                _ => null
            };
        }

        return null;
    }

    private static string? GetPlainTextValue(SinkRecord record)
    {
        if (record.Value == null || record.Value.Length == 0)
            return null;

        return Encoding.UTF8.GetString(record.Value);
    }

    private static string GetDefaultTitle(SinkRecord record)
    {
        // Try to generate a title from the record
        if (record.Key != null && record.Key.Length > 0)
        {
            var key = Encoding.UTF8.GetString(record.Key);
            if (!string.IsNullOrWhiteSpace(key))
                return key;
        }

        return $"Post from {record.Topic} at {record.Timestamp:yyyy-MM-dd HH:mm:ss}";
    }
}
