using Kuestenlogik.Surgewave.Client.Native;
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

            // Initialize offsets for each topic (starting from 0)
            foreach (var topic in _topics)
            {
                // For now, start from partition 0 offset 0
                // A full implementation would discover all partitions
                _currentOffsets[(topic, 0)] = 0;
            }
        }
    }

    public override void Stop()
    {
        _sourceClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _metrics?.Dispose();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_sourceClient == null || _topics.Count == 0)
            return [];

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
                        SourcePartition = new Dictionary<string, object>
                        {
                            ["cluster"] = _sourceClusterAlias,
                            ["topic"] = topic,
                            ["partition"] = partition
                        },
                        SourceOffset = new Dictionary<string, object>
                        {
                            ["offset"] = msg.Offset
                        },
                        Topic = targetTopic,
                        Partition = partition, // Preserve partition
                        Key = msg.Key is { Length: > 0 } ? msg.Key.ToArray() : null,
                        Value = msg.Value?.ToArray() ?? [],
                        Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp),
                        Headers = null // Native protocol doesn't expose headers directly yet
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
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }
        catch (Exception ex)
        {
            _metrics?.RecordError("unknown", ex.GetType().Name);
            Context.RaiseError(ex);
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
