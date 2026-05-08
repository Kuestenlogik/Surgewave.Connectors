using System.Text;
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
            ? batchSize : SurgewaveBridgeConnectorConfig.DefaultBatchSize.ToString());
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_targetClient == null)
        {
            var parts = _targetBootstrapServers.Split(':');
            var host = parts[0];
            var port = parts.Length > 1 ? int.Parse(parts[1]) : 9092;
            _targetClient = new SurgewaveNativeClient(host, port);
            await _targetClient.ConnectAsync(cancellationToken);
        }

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            var targetTopic = GetTargetTopic(record.Topic);
            var targetPartition = _preservePartitions ? record.Partition : 0;

            _batch.Add((targetTopic, targetPartition, record.Key, record.Value));

            if (_batch.Count >= _batchSize)
            {
                await FlushBatchAsync(cancellationToken);
            }
        }
    }

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
    }

    private string GetTargetTopic(string sourceTopic)
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
        // Dispose is handled by DisposeAsync
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _targetClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        base.Dispose(disposing);
    }
}
