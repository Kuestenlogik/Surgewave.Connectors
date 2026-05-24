namespace Kuestenlogik.Surgewave.Connector.Mirror.Filters;

/// <summary>
/// Defines topic filtering logic for cross-cluster replication.
/// </summary>
public interface ITopicFilter
{
    /// <summary>
    /// Determines if a topic should be replicated.
    /// </summary>
    bool ShouldReplicate(string topic);

    /// <summary>
    /// Get all topics that should be replicated from the given list.
    /// </summary>
    IReadOnlyList<string> FilterTopics(IEnumerable<string> topics);
}
