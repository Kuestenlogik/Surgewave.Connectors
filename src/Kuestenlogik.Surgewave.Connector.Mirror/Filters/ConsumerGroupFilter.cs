using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Filters;

/// <summary>
/// Filter for consumer groups to sync offsets.
/// </summary>
public sealed class ConsumerGroupFilter
{
    private readonly Regex? _pattern;
    private readonly HashSet<string> _whitelist;
    private readonly HashSet<string> _blacklist;

    public ConsumerGroupFilter(
        string? pattern,
        IReadOnlyList<string>? whitelist,
        IReadOnlyList<string>? blacklist)
    {
        _pattern = !string.IsNullOrWhiteSpace(pattern)
            ? new Regex(pattern, RegexOptions.Compiled | RegexOptions.Singleline)
            : null;
        _whitelist = whitelist != null ? [..whitelist] : [];
        _blacklist = blacklist != null ? [..blacklist] : [];
    }

    public bool ShouldSync(string groupId)
    {
        // Skip internal groups
        if (groupId.StartsWith("connect-", StringComparison.Ordinal))
            return false;

        if (groupId.StartsWith("mirror-maker-", StringComparison.Ordinal))
            return false;

        // Check blacklist first
        if (_blacklist.Contains(groupId))
            return false;

        // If whitelist is specified, group must be in whitelist
        if (_whitelist.Count > 0)
            return _whitelist.Contains(groupId);

        // Otherwise, check against pattern
        if (_pattern != null)
            return _pattern.IsMatch(groupId);

        // No filter configured - sync all groups
        return true;
    }

    public IReadOnlyList<string> FilterGroups(IEnumerable<string> groups)
    {
        return groups.Where(ShouldSync).ToList();
    }

    /// <summary>
    /// Create a filter from MirrorMaker configuration.
    /// </summary>
    public static ConsumerGroupFilter FromConfig(MirrorMakerConfig config)
    {
        return new ConsumerGroupFilter(
            config.GroupsPattern,
            config.GroupsWhitelist,
            config.GroupsBlacklist);
    }
}
