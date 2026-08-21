using System.Globalization;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Connector.Mirror.Failover;
using Kuestenlogik.Surgewave.Connector.Mirror.Metrics;
using Kuestenlogik.Surgewave.Connector.Mirror.Offsets;
using Kuestenlogik.Surgewave.Connector.Mirror.Policies;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mirror;

/// <summary>
/// Task that replicates records from source cluster to target cluster.
/// Uses the Surgewave native client for high-performance replication.
/// </summary>
public sealed class MirrorSourceTask : SourceTask
{
    private string _taskId = "";
    private string _sourceClusterAlias = "";
    private string _targetClusterAlias = "";
    private List<string> _topics = [];
    private IReplicationPolicy _policy = null!;
    private OffsetTranslator _offsetTranslator = null!;
    private MirrorMetrics? _metrics;

    // Surgewave native client for source cluster
    private SurgewaveNativeClient? _sourceClient;

    // Track offsets per topic-partition
    private readonly Dictionary<(string topic, int partition), long> _currentOffsets = [];

    // Stops hammering a source cluster that keeps failing
    private readonly CircuitBreaker _circuitBreaker = new();

    // Configuration
    private int _pollTimeoutMs;
    private int _fetchMaxBytes;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _taskId = GetConfig(config, "task.id", "0");
        _sourceClusterAlias = GetConfig(config, "source.cluster.alias", "source");
        _targetClusterAlias = GetConfig(config, "target.cluster.alias", "target");

        var topicsConfig = GetConfig(config, "topics", "");
        _topics = string.IsNullOrEmpty(topicsConfig)
            ? []
            : [.. topicsConfig.Split(',', StringSplitOptions.RemoveEmptyEntries)];

        _policy = ReplicationPolicyFactory.Create(
            GetConfig(config, "replication.policy.class", "default"),
            GetConfig(config, "replication.policy.separator", "."));

        _offsetTranslator = new OffsetTranslator();
        _metrics = new MirrorMetrics(_sourceClusterAlias, _targetClusterAlias);

        _pollTimeoutMs = int.Parse(GetConfig(config, "consumer.poll.timeout.ms", "1000"));
        _fetchMaxBytes = int.Parse(GetConfig(config, "fetch.max.bytes", "52428800"));

        // Create Surgewave native client for source cluster
        var sourceBootstrap = GetConfig(config, "source.bootstrap.servers", "");
        if (!string.IsNullOrEmpty(sourceBootstrap))
        {
            var (host, port) = ParseBootstrapServers(sourceBootstrap);
            _sourceClient = new SurgewaveNativeClient(host, port);
            _sourceClient.ConnectAsync().GetAwaiter().GetResult();

            InitializeOffsets();
        }
    }

    /// <summary>
    /// Seed the fetch cursor for every partition of every assigned topic, resuming from the
    /// offset that was committed before the last restart. Without the partition discovery
    /// every partition but 0 would never be replicated; without the offset restore every
    /// restart would replicate each topic from the beginning again.
    /// </summary>
    private void InitializeOffsets()
    {
        foreach (var topic in _topics)
        {
            var partitionCount = GetPartitionCount(topic);
            for (var partition = 0; partition < partitionCount; partition++)
            {
                _currentOffsets[(topic, partition)] = RestoreOffset(topic, partition);
            }
        }
    }

    private int GetPartitionCount(string topic)
    {
        try
        {
            var description = _sourceClient!.Topics.DescribeAsync(topic).GetAwaiter().GetResult();
            return Math.Max(1, description.PartitionCount);
        }
        catch (Exception ex)
        {
            // Metadata unavailable - keep replicating partition 0 rather than nothing, but make
            // the degraded discovery visible instead of silently dropping the other partitions.
            _metrics?.RecordError(topic, ex.GetType().Name);
            Context?.RaiseError?.Invoke(ex);
            return 1;
        }
    }

    private long RestoreOffset(string topic, int partition)
    {
        var storedOffset = Context?.OffsetStorageReader?.Offset(CreateSourcePartition(topic, partition));
        if (storedOffset != null && storedOffset.TryGetValue("offset", out var value) && value != null)
        {
            // The stored value is the offset of the last replicated record - resume after it.
            return Convert.ToInt64(value, CultureInfo.InvariantCulture) + 1;
        }

        return 0;
    }

    private Dictionary<string, object> CreateSourcePartition(string topic, int partition) => new()
    {
        ["cluster"] = _sourceClusterAlias,
        ["topic"] = topic,
        ["partition"] = partition
    };

    public override void Stop()
    {
        _sourceClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _metrics?.Dispose();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_sourceClient == null || _topics.Count == 0)
            return [];

        if (!_circuitBreaker.AllowRequest())
        {
            // Source cluster keeps failing - back off instead of hammering it on every poll.
            await Task.Delay(Math.Min(100, _pollTimeoutMs), cancellationToken);
            return [];
        }

        var records = new List<SourceRecord>();
        var startTime = DateTime.UtcNow;

        try
        {
            // Fetch from each topic-partition
            foreach (var ((topic, partition), offset) in _currentOffsets.ToList())
            {
                var result = await _sourceClient.Messaging.ReceiveAsync(
                    topic, partition, offset, _fetchMaxBytes, maxWaitMs: 100, cancellationToken);

                foreach (var msg in result.Messages)
                {
                    // Transform topic name using replication policy
                    var targetTopic = _policy.FormatRemoteTopic(_sourceClusterAlias, topic);

                    // Create source record
                    var record = new SourceRecord
                    {
                        SourcePartition = CreateSourcePartition(topic, partition),
                        SourceOffset = new Dictionary<string, object>
                        {
                            ["offset"] = msg.Offset
                        },
                        Topic = targetTopic,
                        Partition = partition, // Preserve partition
                        Key = msg.Key is { Length: > 0 } ? msg.Key.ToArray() : null,
                        Value = msg.Value?.ToArray() ?? [],
                        Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp),
                        // Replicate headers verbatim - dropping them loses routing/tracing metadata
                        Headers = msg.Headers is { Count: > 0 } ? new Dictionary<string, byte[]>(msg.Headers) : null
                    };

                    records.Add(record);

                    // Update current offset
                    _currentOffsets[(topic, partition)] = msg.Offset + 1;
                }

                // Track metrics
                if (result.Messages.Count > 0)
                {
                    var totalBytes = result.Messages.Sum(m => (m.Key?.Length ?? 0) + (m.Value?.Length ?? 0));
                    _metrics?.RecordReplicated(topic, partition, result.Messages.Count, totalBytes);
                }
            }

            var latencyMs = (DateTime.UtcNow - startTime).TotalMilliseconds;
            if (records.Count > 0)
            {
                _metrics?.RecordLatency(_topics[0], latencyMs);
            }

            _circuitBreaker.RecordSuccess();
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }
        catch (Exception ex)
        {
            _circuitBreaker.RecordFailure();
            _metrics?.RecordError("unknown", ex.GetType().Name);
            Context?.RaiseError?.Invoke(ex);
        }

        // If no records, wait a bit before polling again
        if (records.Count == 0)
        {
            await Task.Delay(Math.Min(100, _pollTimeoutMs), cancellationToken);
        }

        return records;
    }

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        // Store offset mapping for checkpoint connector
        var sourceTopic = record.SourcePartition["topic"].ToString()!;
        var sourcePartition = Convert.ToInt32(record.SourcePartition["partition"]);
        var sourceOffset = Convert.ToInt64(record.SourceOffset["offset"]);

        _offsetTranslator.StoreMapping(
            _sourceClusterAlias,
            sourceTopic,
            sourcePartition,
            sourceOffset,
            metadata.Offset);
    }

    private static (string host, int port) ParseBootstrapServers(string servers)
    {
        var parts = servers.Split(':');
        return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 9092);
    }

    private static string GetConfig(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _sourceClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _metrics?.Dispose();
        }
        base.Dispose(disposing);
    }
}
