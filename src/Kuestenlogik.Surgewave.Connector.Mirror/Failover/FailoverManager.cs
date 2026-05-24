using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Connector.Mirror.Offsets;
using Kuestenlogik.Surgewave.Connector.Mirror.Policies;

namespace Kuestenlogik.Surgewave.Connector.Mirror.Failover;

/// <summary>
/// Manages failover operations between source and target clusters.
/// Handles consumer group offset migration during failover scenarios.
/// </summary>
public sealed class FailoverManager : IAsyncDisposable
{
    private readonly string _sourceClusterAlias;
    private readonly string _targetClusterAlias;
    private readonly IReplicationPolicy _policy;
    private readonly OffsetTranslator _offsetTranslator;
    private readonly FailoverState _state;

    private SurgewaveNativeClient? _sourceClient;
    private SurgewaveNativeClient? _targetClient;

    public FailoverManager(
        string sourceClusterAlias,
        string targetClusterAlias,
        IReplicationPolicy policy,
        OffsetTranslator offsetTranslator)
    {
        _sourceClusterAlias = sourceClusterAlias;
        _targetClusterAlias = targetClusterAlias;
        _policy = policy;
        _offsetTranslator = offsetTranslator;
        _state = new FailoverState();
    }

    /// <summary>
    /// Current failover state.
    /// </summary>
    public FailoverState State => _state;

    /// <summary>
    /// Connect to both source and target clusters.
    /// </summary>
    public async Task ConnectAsync(string sourceBootstrap, string targetBootstrap,
        CancellationToken cancellationToken = default)
    {
        var (sourceHost, sourcePort) = ParseBootstrapServers(sourceBootstrap);
        var (targetHost, targetPort) = ParseBootstrapServers(targetBootstrap);

        _sourceClient = new SurgewaveNativeClient(sourceHost, sourcePort);
        _targetClient = new SurgewaveNativeClient(targetHost, targetPort);

        await Task.WhenAll(
            _sourceClient.ConnectAsync(cancellationToken),
            _targetClient.ConnectAsync(cancellationToken));

        _state.IsConnected = true;
    }

    /// <summary>
    /// Initiate failover for a consumer group from source to target cluster.
    /// </summary>
    public async Task<FailoverResult> FailoverGroupAsync(
        string consumerGroup,
        IReadOnlyList<TopicPartition> topicPartitions,
        CancellationToken cancellationToken = default)
    {
        if (!_state.IsConnected)
            throw new InvalidOperationException("Not connected to clusters");

        var result = new FailoverResult
        {
            ConsumerGroup = consumerGroup,
            SourceCluster = _sourceClusterAlias,
            TargetCluster = _targetClusterAlias,
            StartedAt = DateTimeOffset.UtcNow
        };

        _state.CurrentFailover = result;
        _state.IsFailoverInProgress = true;

        try
        {
            var offsetMappings = new List<OffsetMapping>();

            foreach (var tp in topicPartitions)
            {
                // Translate source offset to target offset
                var targetOffset = _offsetTranslator.Translate(
                    _sourceClusterAlias, tp.Topic, tp.Partition, tp.Offset);

                if (targetOffset.HasValue)
                {
                    offsetMappings.Add(new OffsetMapping
                    {
                        SourceTopic = tp.Topic,
                        TargetTopic = _policy.FormatRemoteTopic(_sourceClusterAlias, tp.Topic),
                        Partition = tp.Partition,
                        SourceOffset = tp.Offset,
                        TargetOffset = targetOffset.Value
                    });
                }
                else
                {
                    // No mapping available - use latest available estimate
                    result.Warnings.Add($"No offset mapping for {tp.Topic}:{tp.Partition}");
                }
            }

            result.OffsetMappings = offsetMappings;
            result.Success = true;
            result.CompletedAt = DateTimeOffset.UtcNow;
        }
        catch (Exception ex)
        {
            result.Success = false;
            result.Error = ex.Message;
            result.CompletedAt = DateTimeOffset.UtcNow;
        }
        finally
        {
            _state.IsFailoverInProgress = false;
            _state.LastFailover = result;
        }

        return result;
    }

    /// <summary>
    /// Check cluster health and determine if failover is needed.
    /// </summary>
    public Task<ClusterHealthStatus> CheckHealthAsync(CancellationToken cancellationToken = default)
    {
        var status = new ClusterHealthStatus
        {
            CheckedAt = DateTimeOffset.UtcNow
        };

        // Check source cluster - verify connection state
        status.SourceClusterHealthy = _sourceClient?.IsConnected ?? false;

        // Check target cluster
        status.TargetClusterHealthy = _targetClient?.IsConnected ?? false;

        status.ShouldFailover = !status.SourceClusterHealthy && status.TargetClusterHealthy;

        return Task.FromResult(status);
    }

    public async ValueTask DisposeAsync()
    {
        if (_sourceClient != null)
            await _sourceClient.DisposeAsync();
        if (_targetClient != null)
            await _targetClient.DisposeAsync();
    }

    private static (string host, int port) ParseBootstrapServers(string servers)
    {
        var parts = servers.Split(':');
        return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 9092);
    }
}

/// <summary>
/// Tracks the current state of failover operations.
/// </summary>
public sealed class FailoverState
{
    public bool IsConnected { get; set; }
    public bool IsFailoverInProgress { get; set; }
    public FailoverResult? CurrentFailover { get; set; }
    public FailoverResult? LastFailover { get; set; }
}

/// <summary>
/// Result of a failover operation.
/// </summary>
public sealed class FailoverResult
{
    public required string ConsumerGroup { get; init; }
    public required string SourceCluster { get; init; }
    public required string TargetCluster { get; init; }
    public required DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; set; }
    public bool Success { get; set; }
    public string? Error { get; set; }
    public IReadOnlyList<OffsetMapping> OffsetMappings { get; set; } = [];
    public List<string> Warnings { get; } = [];
}

/// <summary>
/// Mapping of source to target offsets for a topic-partition.
/// </summary>
public sealed class OffsetMapping
{
    public required string SourceTopic { get; init; }
    public required string TargetTopic { get; init; }
    public required int Partition { get; init; }
    public required long SourceOffset { get; init; }
    public required long TargetOffset { get; init; }
}

/// <summary>
/// Health status of source and target clusters.
/// </summary>
public sealed class ClusterHealthStatus
{
    public DateTimeOffset CheckedAt { get; init; }
    public bool SourceClusterHealthy { get; set; }
    public bool TargetClusterHealthy { get; set; }
    public bool ShouldFailover { get; set; }
}

/// <summary>
/// Represents a topic-partition with offset.
/// </summary>
public readonly record struct TopicPartition(string Topic, int Partition, long Offset);
