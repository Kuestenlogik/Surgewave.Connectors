using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Surgewave.Bridge;

/// <summary>
/// Task that replicates messages from a source Surgewave cluster.
/// </summary>
public sealed class SurgewaveBridgeSourceTask : SourceTask
{
    /// <summary>Key under which a record's replication cursor lives in <see cref="SourceRecord.SourceOffset"/>.</summary>
    private const string OffsetKey = "offset";

    private SurgewaveNativeClient? _sourceClient;
    private string _sourceBootstrapServers = null!;
    private string _sourceClusterAlias = null!;
    private string _targetTopicTemplate = null!;
    private List<string> _topics = [];
    private Regex? _topicsPattern;
    private HashSet<string> _topicsBlacklist = [];
    private bool _topicPrefixEnabled;
    private string _topicPrefixSeparator = null!;
    private bool _preservePartitions;
    private bool _startFromLatest;
    private int _batchSize;
    private int _pollTimeoutMs;
    private bool _heartbeatEnabled;
    private int _heartbeatIntervalMs;
    private DateTime _lastHeartbeat = DateTime.MinValue;
    private bool _offsetTrackingEnabled;
    private int _offsetSyncIntervalMs;
    private DateTime _lastOffsetSync = DateTime.MinValue;

    private readonly ConcurrentDictionary<(string topic, int partition), long> _currentOffsets = new();
    private readonly ConcurrentDictionary<(string topic, int partition), long> _committedOffsets = new();
    private readonly ConcurrentQueue<SourceRecord> _pendingRecords = new();
    private long _messageId;
    private bool _initialized;
    private CancellationTokenSource? _cts;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _sourceBootstrapServers = config[SurgewaveBridgeConnectorConfig.SourceBootstrapServers];
        _sourceClusterAlias = config.TryGetValue(SurgewaveBridgeConnectorConfig.SourceClusterAlias, out var sourceClusterAlias)
            ? sourceClusterAlias : SurgewaveBridgeConnectorConfig.DefaultSourceClusterAlias;
        _targetTopicTemplate = config[SurgewaveBridgeConnectorConfig.Topic];

        // Parse topics
        if (config.TryGetValue(SurgewaveBridgeConnectorConfig.Topics, out var topicsStr) && !string.IsNullOrWhiteSpace(topicsStr))
        {
            _topics = topicsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        }

        if (config.TryGetValue(SurgewaveBridgeConnectorConfig.TopicsPattern, out var pattern) && !string.IsNullOrWhiteSpace(pattern))
        {
            _topicsPattern = new Regex(pattern, RegexOptions.Compiled);
        }

        if (config.TryGetValue(SurgewaveBridgeConnectorConfig.TopicsBlacklist, out var blacklist) && !string.IsNullOrWhiteSpace(blacklist))
        {
            _topicsBlacklist = blacklist.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToHashSet();
        }

        _topicPrefixEnabled = (config.TryGetValue(SurgewaveBridgeConnectorConfig.TopicPrefixEnabled, out var topicPrefixEnabled) ? topicPrefixEnabled : "true") == "true";
        _topicPrefixSeparator = config.TryGetValue(SurgewaveBridgeConnectorConfig.TopicPrefixSeparator, out var topicPrefixSeparator)
            ? topicPrefixSeparator : SurgewaveBridgeConnectorConfig.DefaultTopicPrefixSeparator;
        _preservePartitions = (config.TryGetValue(SurgewaveBridgeConnectorConfig.PreservePartitions, out var preservePartitions) ? preservePartitions : "true") == "true";
        _startFromLatest = (config.TryGetValue(SurgewaveBridgeConnectorConfig.StartFromLatest, out var startFromLatest) ? startFromLatest : "false") == "true";
        _batchSize = int.Parse(config.TryGetValue(SurgewaveBridgeConnectorConfig.BatchSize, out var batchSize)
            ? batchSize : SurgewaveBridgeConnectorConfig.DefaultBatchSize.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        _pollTimeoutMs = int.Parse(config.TryGetValue(SurgewaveBridgeConnectorConfig.PollTimeoutMs, out var pollTimeoutMs)
            ? pollTimeoutMs : SurgewaveBridgeConnectorConfig.DefaultPollTimeoutMs.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        _heartbeatEnabled = (config.TryGetValue(SurgewaveBridgeConnectorConfig.HeartbeatEnabled, out var heartbeatEnabled) ? heartbeatEnabled : "true") == "true";
        _heartbeatIntervalMs = int.Parse(config.TryGetValue(SurgewaveBridgeConnectorConfig.HeartbeatIntervalMs, out var heartbeatIntervalMs)
            ? heartbeatIntervalMs : SurgewaveBridgeConnectorConfig.DefaultHeartbeatIntervalMs.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        _offsetTrackingEnabled = (config.TryGetValue(SurgewaveBridgeConnectorConfig.OffsetTrackingEnabled, out var offsetTrackingEnabled) ? offsetTrackingEnabled : "true") == "true";
        _offsetSyncIntervalMs = int.Parse(config.TryGetValue(SurgewaveBridgeConnectorConfig.OffsetSyncIntervalMs, out var offsetSyncIntervalMs)
            ? offsetSyncIntervalMs : SurgewaveBridgeConnectorConfig.DefaultOffsetSyncIntervalMs.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);

        _cts = new CancellationTokenSource();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (!_initialized)
        {
            await InitializeAsync(cancellationToken);
            _initialized = true;
        }

        var records = new List<SourceRecord>();

        // Emit heartbeat if needed
        if (_heartbeatEnabled && (DateTime.UtcNow - _lastHeartbeat).TotalMilliseconds >= _heartbeatIntervalMs)
        {
            records.Add(CreateHeartbeatRecord());
            _lastHeartbeat = DateTime.UtcNow;
        }

        // Emit an offset-sync checkpoint of what has actually been committed downstream
        if (_offsetTrackingEnabled && _offsetSyncIntervalMs > 0 &&
            (DateTime.UtcNow - _lastOffsetSync).TotalMilliseconds >= _offsetSyncIntervalMs)
        {
            var checkpoint = CreateCheckpointRecord();
            if (checkpoint != null)
                records.Add(checkpoint);

            _lastOffsetSync = DateTime.UtcNow;
        }

        // Drain pending records
        while (_pendingRecords.TryDequeue(out var pending) && records.Count < _batchSize)
        {
            records.Add(pending);
        }

        if (records.Count >= _batchSize)
            return records;

        // Poll source cluster for each topic/partition
        foreach (var (key, offset) in _currentOffsets.ToArray())
        {
            if (records.Count >= _batchSize)
                break;

            try
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(_pollTimeoutMs);

                var result = await _sourceClient!.Messaging.ReceiveAsync(
                    key.topic, key.partition, offset,
                    maxBytes: 1024 * 1024,
                    maxWaitMs: Math.Min(_pollTimeoutMs, 1000),
                    timeoutCts.Token);

                foreach (var msg in result.Messages)
                {
                    var targetTopic = GetTargetTopic(key.topic);
                    var targetPartition = _preservePartitions ? key.partition : (int?)null;

                    var record = new SourceRecord
                    {
                        SourcePartition = CreateSourcePartition(key.topic, key.partition),
                        SourceOffset = new Dictionary<string, object>
                        {
                            [OffsetKey] = msg.Offset
                        },
                        Topic = targetTopic,
                        Partition = targetPartition,
                        Key = msg.Key,
                        Value = msg.Value,
                        Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp),
                        Headers = new Dictionary<string, byte[]>
                        {
                            ["source.cluster"] = Encoding.UTF8.GetBytes(_sourceClusterAlias),
                            ["source.topic"] = Encoding.UTF8.GetBytes(key.topic),
                            ["source.partition"] = Encoding.UTF8.GetBytes(key.partition.ToString(CultureInfo.InvariantCulture)),
                            ["source.offset"] = Encoding.UTF8.GetBytes(msg.Offset.ToString(CultureInfo.InvariantCulture))
                        }
                    };

                    records.Add(record);
                    _currentOffsets[key] = msg.Offset + 1;

                    if (records.Count >= _batchSize)
                        break;
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // Poll timeout, continue to next partition
            }
            catch (Exception ex)
            {
                // Surface so a partition that keeps failing to replicate stays visible,
                // then continue with the remaining partitions.
                Context?.RaiseError?.Invoke(ex);
            }
        }

        return records;
    }

    private async Task InitializeAsync(CancellationToken cancellationToken)
    {
        // Connect to source cluster
        var parts = _sourceBootstrapServers.Split(':');
        var host = parts[0];
        var port = parts.Length > 1 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 9092;
        _sourceClient = new SurgewaveNativeClient(host, port);
        await _sourceClient.ConnectAsync(cancellationToken);

        // Discover topics
        var allTopics = await _sourceClient.Topics.ListAsync(cancellationToken);
        var topicsToReplicate = new List<string>();

        foreach (var topicInfo in allTopics)
        {
            var topic = topicInfo.Name;

            // Skip internal topics
            if (topic.StartsWith('_'))
                continue;

            // Check blacklist
            if (_topicsBlacklist.Contains(topic))
                continue;

            // Check explicit list
            if (_topics.Count > 0 && _topics.Contains(topic))
            {
                topicsToReplicate.Add(topic);
                continue;
            }

            // Check pattern
            if (_topicsPattern?.IsMatch(topic) == true)
            {
                topicsToReplicate.Add(topic);
            }
        }

        // Initialize offsets for each topic/partition
        foreach (var topic in topicsToReplicate)
        {
            var topicDesc = await _sourceClient.Topics.DescribeAsync(topic, cancellationToken);

            for (int partition = 0; partition < topicDesc.PartitionCount; partition++)
            {
                _currentOffsets[(topic, partition)] = await ResolveStartOffsetAsync(topic, partition, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Resumes a partition where the previous run left off, falling back to the configured
    /// earliest/latest start when nothing has been committed for it yet.
    /// </summary>
    internal async Task<long> ResolveStartOffsetAsync(string topic, int partition, CancellationToken cancellationToken)
    {
        if (_offsetTrackingEnabled)
        {
            var stored = Context?.OffsetStorageReader?.Offset(CreateSourcePartition(topic, partition));

            if (stored != null && stored.TryGetValue(OffsetKey, out var storedOffset) && storedOffset != null)
            {
                var lastReplicated = Convert.ToInt64(storedOffset, CultureInfo.InvariantCulture);
                _committedOffsets[(topic, partition)] = lastReplicated;

                // The stored value is the offset of the last replicated message
                return lastReplicated + 1;
            }
        }

        return _startFromLatest
            ? await _sourceClient!.Messaging.GetLatestOffsetAsync(topic, partition, cancellationToken)
            : 0;
    }

    private Dictionary<string, object> CreateSourcePartition(string topic, int partition) => new()
    {
        ["cluster"] = _sourceClusterAlias,
        ["topic"] = topic,
        ["partition"] = partition
    };

    internal string GetTargetTopic(string sourceTopic)
    {
        var result = _targetTopicTemplate;

        if (result.Contains("${source.topic}"))
        {
            var targetName = _topicPrefixEnabled
                ? $"{_sourceClusterAlias}{_topicPrefixSeparator}{sourceTopic}"
                : sourceTopic;
            result = result.Replace("${source.topic}", targetName);
        }
        else if (_topicPrefixEnabled)
        {
            result = $"{_sourceClusterAlias}{_topicPrefixSeparator}{result}";
        }

        return result;
    }

    private SourceRecord CreateHeartbeatRecord()
    {
        var heartbeat = new
        {
            source_cluster = _sourceClusterAlias,
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            topics_count = _currentOffsets.Keys.Select(k => k.topic).Distinct().Count(),
            partitions_count = _currentOffsets.Count
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["type"] = "heartbeat" },
            SourceOffset = new Dictionary<string, object> { ["id"] = Interlocked.Increment(ref _messageId) },
            Topic = $"{_sourceClusterAlias}.heartbeats",
            Key = Encoding.UTF8.GetBytes(_sourceClusterAlias),
            Value = JsonSerializer.SerializeToUtf8Bytes(heartbeat),
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    /// <summary>
    /// Builds a checkpoint of the source offsets that are confirmed replicated, so a
    /// failover target can pick the replication up where this task left it.
    /// </summary>
    internal SourceRecord? CreateCheckpointRecord()
    {
        if (_committedOffsets.IsEmpty)
            return null;

        var checkpoint = new
        {
            source_cluster = _sourceClusterAlias,
            timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            offsets = _committedOffsets
                .ToArray()
                .Select(kvp => new
                {
                    topic = kvp.Key.topic,
                    partition = kvp.Key.partition,
                    offset = kvp.Value
                })
                .ToArray()
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["type"] = "checkpoint" },
            SourceOffset = new Dictionary<string, object> { ["id"] = Interlocked.Increment(ref _messageId) },
            Topic = $"{_sourceClusterAlias}.checkpoints",
            Key = Encoding.UTF8.GetBytes(_sourceClusterAlias),
            Value = JsonSerializer.SerializeToUtf8Bytes(checkpoint),
            Timestamp = DateTimeOffset.UtcNow
        };
    }

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        if (!_offsetTrackingEnabled)
            return;

        // Heartbeat and checkpoint records carry no replication cursor
        if (!record.SourcePartition.TryGetValue("topic", out var topicValue) ||
            !record.SourcePartition.TryGetValue("partition", out var partitionValue) ||
            !record.SourceOffset.TryGetValue(OffsetKey, out var offsetValue) ||
            topicValue == null || partitionValue == null || offsetValue == null)
        {
            return;
        }

        var topic = topicValue.ToString();
        if (string.IsNullOrEmpty(topic))
            return;

        var partition = Convert.ToInt32(partitionValue, CultureInfo.InvariantCulture);
        var offset = Convert.ToInt64(offsetValue, CultureInfo.InvariantCulture);

        _committedOffsets.AddOrUpdate((topic, partition), offset, (_, previous) => Math.Max(previous, offset));
    }

    public override void Stop()
    {
        _cts?.Cancel();
        _cts?.Dispose();
        _cts = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _cts?.Dispose();
            _cts = null;
            _sourceClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _sourceClient = null;
        }
        base.Dispose(disposing);
    }
}
