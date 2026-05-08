namespace Kuestenlogik.Surgewave.Connector.Mirror.Policies;

/// <summary>
/// Default replication policy that prefixes topics with the source cluster alias.
/// Topic naming: "{sourceClusterAlias}{separator}{originalTopic}"
/// Example: "dc1.orders" for topic "orders" from cluster "dc1"
/// </summary>
public sealed class DefaultReplicationPolicy : IReplicationPolicy
{
    private const string HeartbeatSuffix = ".heartbeats";
    private const string CheckpointSuffix = ".checkpoints.internal";
    private const string OffsetSyncSuffix = ".offset-syncs.internal";

    public string Separator { get; }

    public DefaultReplicationPolicy(string separator = ".")
    {
        Separator = separator;
    }

    public string FormatRemoteTopic(string sourceClusterAlias, string topic)
    {
        // Don't double-prefix if already from another cluster
        if (TopicSource(topic) != null)
            return topic;

        return $"{sourceClusterAlias}{Separator}{topic}";
    }

    public string OriginalTopic(string topic)
    {
        var source = TopicSource(topic);
        if (source == null)
            return topic;

        var prefix = source + Separator;
        return topic[prefix.Length..];
    }

    public string? TopicSource(string topic)
    {
        // Internal topics don't have a source cluster in the same way
        if (IsInternalTopic(topic))
            return null;

        var separatorIndex = topic.IndexOf(Separator, StringComparison.Ordinal);
        if (separatorIndex <= 0)
            return null;

        return topic[..separatorIndex];
    }

    public bool IsFromCluster(string topic, string clusterAlias)
    {
        return topic.StartsWith($"{clusterAlias}{Separator}", StringComparison.Ordinal);
    }

    public bool IsInternalTopic(string topic)
    {
        return IsHeartbeatTopic(topic) ||
               IsCheckpointTopic(topic) ||
               topic.EndsWith(OffsetSyncSuffix, StringComparison.Ordinal) ||
               topic.StartsWith("__", StringComparison.Ordinal); // Kafka internal topics
    }

    /// <summary>
    /// Check if replicating this topic would create a loop.
    /// A loop occurs when trying to replicate a topic that already came from the target cluster.
    /// </summary>
    public bool WouldCreateLoop(string topic, string sourceClusterAlias, string targetClusterAlias)
    {
        // Don't replicate topics that originated from the target cluster
        // This prevents: dc1 -> dc2 -> dc1 (infinite loop)
        return IsFromCluster(topic, targetClusterAlias);
    }

    /// <summary>
    /// Get the upstream topic path for a replicated topic.
    /// Returns the chain of clusters the topic has passed through.
    /// </summary>
    public IReadOnlyList<string> GetUpstreamPath(string topic)
    {
        var path = new List<string>();
        var current = topic;

        while (TopicSource(current) is { } source)
        {
            path.Add(source);
            current = OriginalTopic(current);
        }

        return path;
    }

    public bool IsCheckpointTopic(string topic)
    {
        return topic.EndsWith(CheckpointSuffix, StringComparison.Ordinal);
    }

    public bool IsHeartbeatTopic(string topic)
    {
        return topic.EndsWith(HeartbeatSuffix, StringComparison.Ordinal);
    }

    public string HeartbeatTopic(string clusterAlias)
    {
        return $"{clusterAlias}{HeartbeatSuffix}";
    }

    public string CheckpointTopic(string sourceClusterAlias, string targetClusterAlias)
    {
        return $"{sourceClusterAlias}->{targetClusterAlias}{CheckpointSuffix}";
    }

    public string OffsetSyncTopic(string clusterAlias)
    {
        return $"mm2-offset-syncs{Separator}{clusterAlias}{OffsetSyncSuffix}";
    }
}
