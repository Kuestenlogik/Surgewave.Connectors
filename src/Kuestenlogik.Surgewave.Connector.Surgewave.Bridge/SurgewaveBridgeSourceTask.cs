using System.Collections.Concurrent;
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

    private readonly ConcurrentDictionary<(string topic, int partition), long> _currentOffsets = new();
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
            ? batchSize : SurgewaveBridgeConnectorConfig.DefaultBatchSize.ToString());
        _pollTimeoutMs = int.Parse(config.TryGetValue(SurgewaveBridgeConnectorConfig.PollTimeoutMs, out var pollTimeoutMs)
            ? pollTimeoutMs : SurgewaveBridgeConnectorConfig.DefaultPollTimeoutMs.ToString());
        _heartbeatEnabled = (config.TryGetValue(SurgewaveBridgeConnectorConfig.HeartbeatEnabled, out var heartbeatEnabled) ? heartbeatEnabled : "true") == "true";
        _heartbeatIntervalMs = int.Parse(config.TryGetValue(SurgewaveBridgeConnectorConfig.HeartbeatIntervalMs, out var heartbeatIntervalMs)
            ? heartbeatIntervalMs : SurgewaveBridgeConnectorConfig.DefaultHeartbeatIntervalMs.ToString());

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
                        SourcePartition = new Dictionary<string, object>
                        {
                            ["cluster"] = _sourceClusterAlias,
                            ["topic"] = key.topic,
                            ["partition"] = key.partition
                        },
                        SourceOffset = new Dictionary<string, object>
                        {
                            ["offset"] = msg.Offset
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
                            ["source.partition"] = Encoding.UTF8.GetBytes(key.partition.ToString()),
                            ["source.offset"] = Encoding.UTF8.GetBytes(msg.Offset.ToString())
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
            catch (Exception)
            {
                // Log and continue
            }
        }

        return records;
    }

    private async Task InitializeAsync(CancellationToken cancellationToken)
    {
        // Connect to source cluster
        var parts = _sourceBootstrapServers.Split(':');
        var host = parts[0];
        var port = parts.Length > 1 ? int.Parse(parts[1]) : 9092;
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
                long startOffset = 0;
                if (_startFromLatest)
                {
                    startOffset = await _sourceClient.Messaging.GetLatestOffsetAsync(topic, partition, cancellationToken);
                }

                _currentOffsets[(topic, partition)] = startOffset;
            }
        }
    }

    private string GetTargetTopic(string sourceTopic)
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

    public override void CommitRecord(SourceRecord record, RecordMetadata metadata)
    {
        // Offset tracking for failover could be implemented here
    }

    public override void Stop()
    {
        _cts?.Cancel();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _sourceClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _cts?.Dispose();
        }
        base.Dispose(disposing);
    }
}
