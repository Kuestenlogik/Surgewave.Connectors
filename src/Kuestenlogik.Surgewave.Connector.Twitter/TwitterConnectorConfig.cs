namespace Kuestenlogik.Surgewave.Connector.Twitter;

/// <summary>
/// Configuration constants for the Twitter/X connector.
/// </summary>
public static class TwitterConnectorConfig
{
    // Common
    public const string Topic = "topic";

    // OAuth credentials
    public const string ConsumerKey = "twitter.consumer.key";
    public const string ConsumerSecret = "twitter.consumer.secret";
    public const string AccessToken = "twitter.access.token";
    public const string AccessTokenSecret = "twitter.access.token.secret";
    public const string BearerToken = "twitter.bearer.token";

    // Source settings
    public const string SearchQuery = "twitter.search.query";
    public const string UserIds = "twitter.user.ids";
    public const string StreamRules = "twitter.stream.rules";
    public const string PollIntervalMs = "twitter.poll.interval.ms";
    public const string MaxResults = "twitter.max.results";
    public const string IncludeRetweets = "twitter.include.retweets";
    public const string IncludeReplies = "twitter.include.replies";

    // Sink settings
    public const string TextField = "twitter.text.field";
    public const string ReplyToField = "twitter.reply.to.field";
    public const string QuoteTweetField = "twitter.quote.tweet.field";

    // Defaults
    public const int DefaultPollIntervalMs = 15000; // Rate limits
    public const int DefaultMaxResults = 100;
    public const bool DefaultIncludeRetweets = true;
    public const bool DefaultIncludeReplies = true;
}
