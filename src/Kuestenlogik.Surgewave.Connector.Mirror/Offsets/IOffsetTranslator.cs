namespace Kuestenlogik.Surgewave.Connector.Mirror.Offsets;

/// <summary>
/// Translates offsets between source and target clusters.
/// </summary>
public interface IOffsetTranslator
{
    /// <summary>
    /// Store an offset mapping from source to target.
    /// </summary>
    void StoreMapping(string sourceCluster, string topic, int partition, long sourceOffset, long targetOffset);

    /// <summary>
    /// Translate a source offset to a target offset.
    /// Returns null if no mapping exists.
    /// </summary>
    long? Translate(string sourceCluster, string topic, int partition, long sourceOffset);

    /// <summary>
    /// Get the latest target offset for a topic-partition.
    /// </summary>
    long? GetLatestTargetOffset(string sourceCluster, string topic, int partition);

    /// <summary>
    /// Get the latest source offset for a topic-partition.
    /// </summary>
    long? GetLatestSourceOffset(string sourceCluster, string topic, int partition);
}
