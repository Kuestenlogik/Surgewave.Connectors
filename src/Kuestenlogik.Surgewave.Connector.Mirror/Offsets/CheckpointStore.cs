using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Offsets;

/// <summary>
/// Represents a checkpoint record for consumer group offset synchronization.
/// </summary>
public readonly record struct Checkpoint
{
    public required string ConsumerGroup { get; init; }
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long SourceOffset { get; init; }
    public required long TargetOffset { get; init; }
    public required long Timestamp { get; init; }
}

/// <summary>
/// In-memory store for checkpoint data.
/// </summary>
public sealed class CheckpointStore
{
    private readonly ConcurrentDictionary<string, Checkpoint> _checkpoints = new();

    public void Store(Checkpoint checkpoint)
    {
        var key = MakeKey(checkpoint.ConsumerGroup, checkpoint.Topic, checkpoint.Partition);
        _checkpoints[key] = checkpoint;
    }

    public Checkpoint? Get(string consumerGroup, string topic, int partition)
    {
        var key = MakeKey(consumerGroup, topic, partition);
        return _checkpoints.TryGetValue(key, out var checkpoint) ? checkpoint : null;
    }

    public IReadOnlyList<Checkpoint> GetForGroup(string consumerGroup)
    {
        var prefix = $"{consumerGroup}:";
        return _checkpoints
            .Where(kv => kv.Key.StartsWith(prefix, StringComparison.Ordinal))
            .Select(kv => kv.Value)
            .ToList();
    }

    public IReadOnlyList<Checkpoint> All => [.. _checkpoints.Values];

    public void Clear()
    {
        _checkpoints.Clear();
    }

    private static string MakeKey(string consumerGroup, string topic, int partition)
        => $"{consumerGroup}:{topic}:{partition}";
}
