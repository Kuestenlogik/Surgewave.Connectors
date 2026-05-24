namespace Kuestenlogik.Surgewave.Connector.Reddit;

/// <summary>
/// Configuration constants for the Reddit connector.
/// </summary>
public static class RedditConnectorConfig
{
    // Common config
    public const string TopicConfig = "topic";
    public const string TopicsConfig = "topics";

    // Reddit API credentials
    public const string ClientIdConfig = "reddit.client.id";
    public const string ClientSecretConfig = "reddit.client.secret";
    public const string UsernameConfig = "reddit.username";
    public const string PasswordConfig = "reddit.password";
    public const string UserAgentConfig = "reddit.user.agent";

    // Source settings
    public const string SubredditsConfig = "reddit.subreddits";
    public const string PollIntervalMsConfig = "reddit.poll.interval.ms";
    public const string ContentTypeConfig = "reddit.content.type";
    public const string SortByConfig = "reddit.sort.by";
    public const string TimePeriodConfig = "reddit.time.period";
    public const string MaxPostsPerPollConfig = "reddit.max.posts.per.poll";
    public const string IncludeCommentsConfig = "reddit.include.comments";
    public const string MaxCommentsPerPostConfig = "reddit.max.comments.per.post";
    public const string FlairFilterConfig = "reddit.flair.filter";
    public const string NsfwFilterConfig = "reddit.nsfw.filter";

    // Sink settings
    public const string DefaultSubredditConfig = "reddit.default.subreddit";
    public const string SubredditFieldConfig = "reddit.subreddit.field";
    public const string TitleFieldConfig = "reddit.title.field";
    public const string TextFieldConfig = "reddit.text.field";
    public const string UrlFieldConfig = "reddit.url.field";
    public const string FlairFieldConfig = "reddit.flair.field";
    public const string NsfwFieldConfig = "reddit.nsfw.field";
    public const string SpoilerFieldConfig = "reddit.spoiler.field";
    public const string PostTypeConfig = "reddit.post.type";
    public const string ParentIdFieldConfig = "reddit.parent.id.field";
    public const string ReplyToCommentsConfig = "reddit.reply.to.comments";

    // Behavior settings
    public const string BatchSizeConfig = "reddit.batch.size";
    public const string RetryCountConfig = "reddit.retry.count";
    public const string RetryDelayMsConfig = "reddit.retry.delay.ms";

    // Default values
    public const int DefaultPollIntervalMs = 60000; // 1 minute
    public const string DefaultContentType = "posts"; // posts, comments, both
    public const string DefaultSortBy = "new"; // hot, new, top, rising, controversial
    public const string DefaultTimePeriod = "day"; // hour, day, week, month, year, all
    public const int DefaultMaxPostsPerPoll = 25;
    public const bool DefaultIncludeComments = false;
    public const int DefaultMaxCommentsPerPost = 10;
    public const string DefaultNsfwFilter = "include"; // include, exclude, only
    public const string DefaultPostType = "self"; // self, link, crosspost
    public const bool DefaultReplyToComments = false;
    public const int DefaultBatchSize = 10;
    public const int DefaultRetryCount = 3;
    public const int DefaultRetryDelayMs = 1000;

    // Content types
    public const string ContentTypePosts = "posts";
    public const string ContentTypeComments = "comments";
    public const string ContentTypeBoth = "both";

    // Sort options
    public const string SortHot = "hot";
    public const string SortNew = "new";
    public const string SortTop = "top";
    public const string SortRising = "rising";
    public const string SortControversial = "controversial";

    // Time periods
    public const string TimePeriodHour = "hour";
    public const string TimePeriodDay = "day";
    public const string TimePeriodWeek = "week";
    public const string TimePeriodMonth = "month";
    public const string TimePeriodYear = "year";
    public const string TimePeriodAll = "all";

    // NSFW filter options
    public const string NsfwInclude = "include";
    public const string NsfwExclude = "exclude";
    public const string NsfwOnly = "only";

    // Post types
    public const string PostTypeSelf = "self";
    public const string PostTypeLink = "link";
    public const string PostTypeCrosspost = "crosspost";

    // Offset tracking
    public const string OffsetPostId = "post_id";
    public const string OffsetCommentId = "comment_id";
    public const string OffsetSubreddit = "subreddit";
    public const string OffsetTimestamp = "timestamp";
}
