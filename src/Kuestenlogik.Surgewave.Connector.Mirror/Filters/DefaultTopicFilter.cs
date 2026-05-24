using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Connector.Mirror.Policies;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Filters;

/// <summary>
/// Default topic filter combining regex patterns, whitelist, and blacklist.
/// </summary>
public sealed class DefaultTopicFilter : ITopicFilter
{
    private readonly Regex? _pattern;
    private readonly HashSet<string> _whitelist;
    private readonly HashSet<string> _blacklist;
    private readonly IReplicationPolicy _policy;

    public DefaultTopicFilter(
        string? pattern,
        IReadOnlyList<string>? whitelist,
        IReadOnlyList<string>? blacklist,
        IReplicationPolicy policy)
    {
        _pattern = !string.IsNullOrWhiteSpace(pattern)
            ? new Regex(pattern, RegexOptions.Compiled | RegexOptions.Singleline)
            : null;
        _whitelist = whitelist != null ? [..whitelist] : [];
        _blacklist = blacklist != null ? [..blacklist] : [];
        _policy = policy;
    }

    public bool ShouldReplicate(string topic)
    {
        // Skip internal topics
        if (_policy.IsInternalTopic(topic))
            return false;

        // Skip Kafka internal topics
        if (topic.StartsWith("__", StringComparison.Ordinal))
            return false;

        // Check blacklist first (explicit exclude)
        if (_blacklist.Contains(topic))
            return false;

        // If whitelist is specified, topic must be in whitelist
        if (_whitelist.Count > 0)
            return _whitelist.Contains(topic);

        // Otherwise, check against pattern
        if (_pattern != null)
            return _pattern.IsMatch(topic);

        // No filter configured - replicate everything
        return true;
    }

    public IReadOnlyList<string> FilterTopics(IEnumerable<string> topics)
    {
        return topics.Where(ShouldReplicate).ToList();
    }

    /// <summary>
    /// Create a filter from MirrorMaker configuration.
    /// </summary>
    public static DefaultTopicFilter FromConfig(MirrorMakerConfig config, IReplicationPolicy policy)
    {
        return new DefaultTopicFilter(
            config.TopicsPattern,
            config.TopicsWhitelist,
            config.TopicsBlacklist,
            policy);
    }
}
