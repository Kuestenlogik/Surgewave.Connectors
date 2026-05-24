namespace Kuestenlogik.Surgewave.Connector.Mirror.Policies;

/// <summary>
/// Defines topic naming and routing policies for cross-cluster replication.
/// </summary>
public interface IReplicationPolicy
{
    /// <summary>
    /// Format the upstream topic name for the downstream cluster.
    /// Example: "orders" -> "source-cluster.orders"
    /// </summary>
    string FormatRemoteTopic(string sourceClusterAlias, string topic);

    /// <summary>
    /// Extract the original topic name from a replicated topic.
    /// Example: "source-cluster.orders" -> "orders"
    /// </summary>
    string OriginalTopic(string topic);

    /// <summary>
    /// Extract the source cluster alias from a replicated topic.
    /// Example: "source-cluster.orders" -> "source-cluster"
    /// Returns null if the topic is not a replicated topic.
    /// </summary>
    string? TopicSource(string topic);

    /// <summary>
    /// Check if the topic originated from the given cluster.
    /// </summary>
    bool IsFromCluster(string topic, string clusterAlias);

    /// <summary>
    /// Check if a topic is an internal mirroring topic (heartbeats, checkpoints, etc.).
    /// </summary>
    bool IsInternalTopic(string topic);

    /// <summary>
    /// Check if a topic is a checkpoint topic.
    /// </summary>
    bool IsCheckpointTopic(string topic);

    /// <summary>
    /// Check if a topic is a heartbeat topic.
    /// </summary>
    bool IsHeartbeatTopic(string topic);

    /// <summary>
    /// Get the heartbeat topic for a cluster.
    /// </summary>
    string HeartbeatTopic(string clusterAlias);

    /// <summary>
    /// Get the checkpoint topic for source -> target replication.
    /// </summary>
    string CheckpointTopic(string sourceClusterAlias, string targetClusterAlias);

    /// <summary>
    /// Get the offset sync topic for a cluster.
    /// </summary>
    string OffsetSyncTopic(string clusterAlias);

    /// <summary>
    /// Get the separator used between cluster alias and topic name.
    /// </summary>
    string Separator { get; }
}
