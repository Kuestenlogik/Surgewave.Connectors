using System.Globalization;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Surgewave.Bridge;

/// <summary>
/// Task that writes records to a remote Surgewave cluster.
/// </summary>
public sealed class SurgewaveBridgeSinkTask : SinkTask
{
    private SurgewaveNativeClient? _targetClient;
    private string _targetBootstrapServers = null!;
    private string _targetClusterAlias = null!;
    private string? _topicOverride;
    private bool _topicPrefixEnabled;
    private string _topicPrefixSeparator = null!;
    private bool _preservePartitions;
    private int _batchSize;
    private int _lingerMs;
    private DateTime _firstBatchedAtUtc = DateTime.MinValue;
    private long _roundRobin = -1;
    private readonly Dictionary<string, int> _targetPartitionCounts = [];
    private readonly List<(string topic, int partition, byte[]? key, byte[] value)> _batch = [];

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _targetBootstrapServers = config[SurgewaveBridgeConnectorConfig.TargetBootstrapServers];
        _targetClusterAlias = config.TryGetValue(SurgewaveBridgeConnectorConfig.TargetClusterAlias, out var targetClusterAlias)
            ? targetClusterAlias : SurgewaveBridgeConnectorConfig.DefaultTargetClusterAlias;
        _topicOverride = config.TryGetValue(SurgewaveBridgeConnectorConfig.Topic, out var topicOverride) ? topicOverride : null;
        _topicPrefixEnabled = (config.TryGetValue(SurgewaveBridgeConnectorConfig.TopicPrefixEnabled, out var topicPrefixEnabled) ? topicPrefixEnabled : "false") == "true";
        _topicPrefixSeparator = config.TryGetValue(SurgewaveBridgeConnectorConfig.TopicPrefixSeparator, out var topicPrefixSeparator)
            ? topicPrefixSeparator : SurgewaveBridgeConnectorConfig.DefaultTopicPrefixSeparator;
        _preservePartitions = (config.TryGetValue(SurgewaveBridgeConnectorConfig.PreservePartitions, out var preservePartitions) ? preservePartitions : "true") == "true";
        _batchSize = int.Parse(config.TryGetValue(SurgewaveBridgeConnectorConfig.BatchSize, out var batchSize)
            ? batchSize : SurgewaveBridgeConnectorConfig.DefaultBatchSize.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        _lingerMs = int.Parse(config.TryGetValue(SurgewaveBridgeConnectorConfig.ProducerLingerMs, out var lingerMs)
            ? lingerMs : SurgewaveBridgeConnectorConfig.DefaultProducerLingerMs.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_targetClient == null)
        {
            var parts = _targetBootstrapServers.Split(':');
            var host = parts[0];
            var port = parts.Length > 1 ? int.Parse(parts[1], CultureInfo.InvariantCulture) : 9092;
            _targetClient = new SurgewaveNativeClient(host, port);
            await _targetClient.ConnectAsync(cancellationToken);
        }

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            var targetTopic = GetTargetTopic(record.Topic);
            var targetPartition = await ResolveTargetPartitionAsync(targetTopic, record, cancellationToken);

            if (_batch.Count == 0)
                _firstBatchedAtUtc = DateTime.UtcNow;

            _batch.Add((targetTopic, targetPartition, record.Key, record.Value));

            if (_batch.Count >= _batchSize || LingerElapsed())
            {
                await FlushBatchAsync(cancellationToken);
            }
        }
    }

    /// <summary>
    /// Picks the target partition: mirrored when partition preservation is on, otherwise
    /// spread over the target topic - keyed records stick to one partition, unkeyed ones
    /// go round-robin.
    /// </summary>
    private async Task<int> ResolveTargetPartitionAsync(string targetTopic, SinkRecord record, CancellationToken cancellationToken)
    {
        if (_preservePartitions)
            return record.Partition;

        if (!_targetPartitionCounts.TryGetValue(targetTopic, out var partitionCount))
        {
            var description = await _targetClient!.Topics.DescribeAsync(targetTopic, cancellationToken);
            partitionCount = Math.Max(1, description.PartitionCount);
            _targetPartitionCounts[targetTopic] = partitionCount;
        }

        return SelectPartition(record, partitionCount);
    }

    /// <summary>
    /// Spreads a record over the target topic's partitions: keyed records stick to the
    /// partition their key hashes to, unkeyed ones go round-robin.
    /// </summary>
    internal int SelectPartition(SinkRecord record, int partitionCount)
    {
        if (partitionCount == 1)
            return 0;

        if (record.Key is { Length: > 0 })
            return (int)(FnvHash(record.Key) % (uint)partitionCount);

        return (int)(Interlocked.Increment(ref _roundRobin) % partitionCount);
    }

    private static uint FnvHash(ReadOnlySpan<byte> key)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (var b in key)
            {
                hash = (hash ^ b) * 16777619;
            }
            return hash;
        }
    }

    /// <summary>
    /// True once the oldest batched record has been waiting longer than <c>producer.linger.ms</c>.
    /// </summary>
    private bool LingerElapsed()
        => _lingerMs > 0
           && _batch.Count > 0
           && (DateTime.UtcNow - _firstBatchedAtUtc).TotalMilliseconds >= _lingerMs;

    private async Task FlushBatchAsync(CancellationToken cancellationToken)
    {
        if (_batch.Count == 0) return;

        // Group by topic/partition for batch sending
        var grouped = _batch.GroupBy(b => (b.topic, b.partition));

        foreach (var group in grouped)
        {
            var messages = group.Select(g => (g.key, g.value)).ToList();
            await _targetClient!.Messaging.SendBatchAsync(group.Key.topic, group.Key.partition, messages, cancellationToken);
        }

        _batch.Clear();
        _firstBatchedAtUtc = DateTime.MinValue;
    }

    internal string GetTargetTopic(string sourceTopic)
    {
        if (!string.IsNullOrEmpty(_topicOverride))
        {
            var result = _topicOverride.Replace("${topic}", sourceTopic);
            if (_topicPrefixEnabled && !result.StartsWith(_targetClusterAlias, StringComparison.Ordinal))
            {
                result = $"{_targetClusterAlias}{_topicPrefixSeparator}{result}";
            }
            return result;
        }

        if (_topicPrefixEnabled)
        {
            return $"{_targetClusterAlias}{_topicPrefixSeparator}{sourceTopic}";
        }

        return sourceTopic;
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        await FlushBatchAsync(cancellationToken);
    }

    public override void Stop()
    {
        _batch.Clear();
        _firstBatchedAtUtc = DateTime.MinValue;
        _targetPartitionCounts.Clear();
        _targetClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _targetClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _targetClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _targetClient = null;
        }
        base.Dispose(disposing);
    }
}
