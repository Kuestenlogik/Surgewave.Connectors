using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Models;

/// <summary>
/// Heartbeat record emitted by MirrorHeartbeatConnector.
/// </summary>
public sealed record Heartbeat
{
    [JsonPropertyName("sourceCluster")]
    public required string SourceCluster { get; init; }

    [JsonPropertyName("targetCluster")]
    public required string TargetCluster { get; init; }

    [JsonPropertyName("timestamp")]
    public required long Timestamp { get; init; }
}

/// <summary>
/// Checkpoint record emitted by MirrorCheckpointConnector.
/// </summary>
public sealed record CheckpointRecord
{
    [JsonPropertyName("consumerGroup")]
    public required string ConsumerGroup { get; init; }

    [JsonPropertyName("topic")]
    public required string Topic { get; init; }

    [JsonPropertyName("partition")]
    public required int Partition { get; init; }

    [JsonPropertyName("sourceOffset")]
    public required long SourceOffset { get; init; }

    [JsonPropertyName("targetOffset")]
    public required long TargetOffset { get; init; }

    [JsonPropertyName("metadata")]
    public string? Metadata { get; init; }

    [JsonPropertyName("timestamp")]
    public required long Timestamp { get; init; }
}

/// <summary>
/// Offset sync record for tracking source-to-target offset mappings.
/// </summary>
public sealed record OffsetSyncRecord
{
    [JsonPropertyName("topic")]
    public required string Topic { get; init; }

    [JsonPropertyName("partition")]
    public required int Partition { get; init; }

    [JsonPropertyName("sourceOffset")]
    public required long SourceOffset { get; init; }

    [JsonPropertyName("targetOffset")]
    public required long TargetOffset { get; init; }

    [JsonPropertyName("timestamp")]
    public required long Timestamp { get; init; }
}
