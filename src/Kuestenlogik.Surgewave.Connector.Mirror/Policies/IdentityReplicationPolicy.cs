namespace Kuestenlogik.Surgewave.Connector.Mirror.Policies;

/// <summary>
/// Identity replication policy that keeps topic names unchanged.
/// Use this when you don't need cluster prefixing (single-direction replication only).
/// WARNING: Cannot be used for bidirectional replication as it would create infinite loops.
/// </summary>
public sealed class IdentityReplicationPolicy : IReplicationPolicy
{
    private const string HeartbeatsTopicName = "mm2.heartbeats";
    private const string CheckpointsTopicName = "mm2.checkpoints.internal";
    private const string OffsetSyncTopicName = "mm2.offset-syncs.internal";

    public string Separator => ".";

    public string FormatRemoteTopic(string sourceClusterAlias, string topic)
    {
        // Keep topic name unchanged
        return topic;
    }

    public string OriginalTopic(string topic)
    {
        return topic;
    }

    public string? TopicSource(string topic)
    {
        // Identity policy doesn't encode source cluster in topic name
        return null;
    }

    public bool IsFromCluster(string topic, string clusterAlias)
    {
        // Cannot determine source cluster with identity policy
        return false;
    }

    public bool IsInternalTopic(string topic)
    {
        return IsHeartbeatTopic(topic) ||
               IsCheckpointTopic(topic) ||
               topic == OffsetSyncTopicName ||
               topic.StartsWith("__", StringComparison.Ordinal) ||
               topic.StartsWith("mm2.", StringComparison.Ordinal);
    }

    public bool IsCheckpointTopic(string topic)
    {
        return topic == CheckpointsTopicName ||
               topic.EndsWith(".checkpoints.internal", StringComparison.Ordinal);
    }

    public bool IsHeartbeatTopic(string topic)
    {
        return topic == HeartbeatsTopicName ||
               topic.EndsWith(".heartbeats", StringComparison.Ordinal);
    }

    public string HeartbeatTopic(string clusterAlias)
    {
        return $"{clusterAlias}.heartbeats";
    }

    public string CheckpointTopic(string sourceClusterAlias, string targetClusterAlias)
    {
        return $"{sourceClusterAlias}.checkpoints.internal";
    }

    public string OffsetSyncTopic(string clusterAlias)
    {
        return $"{clusterAlias}.offset-syncs.internal";
    }
}
