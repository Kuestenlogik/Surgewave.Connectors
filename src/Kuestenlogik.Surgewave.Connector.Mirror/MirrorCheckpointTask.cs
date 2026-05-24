using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Connector.Mirror.Filters;
using Kuestenlogik.Surgewave.Connector.Mirror.Models;
using Kuestenlogik.Surgewave.Connector.Mirror.Offsets;
using Kuestenlogik.Surgewave.Connector.Mirror.Policies;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mirror;

/// <summary>
/// Task that emits checkpoint records for consumer group offset synchronization.
/// Maps source cluster consumer group offsets to target cluster offsets.
/// </summary>
public sealed class MirrorCheckpointTask : SourceTask
{
    private string _sourceClusterAlias = "";
    private string _targetClusterAlias = "";
    private string _checkpointsTopic = "";
    private int _intervalMs;
    private bool _syncGroupOffsets;
    private IReplicationPolicy _policy = null!;
    private ConsumerGroupFilter _groupFilter = null!;
    private OffsetTranslator _offsetTranslator = null!;
    private CheckpointStore _checkpointStore = null!;
    private DateTime _lastCheckpoint = DateTime.MinValue;

    // Surgewave native client for cluster access
    private SurgewaveNativeClient? _sourceClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _sourceClusterAlias = GetConfig(config, "source.cluster.alias", "source");
        _targetClusterAlias = GetConfig(config, "target.cluster.alias", "target");
        _checkpointsTopic = GetConfig(config, "checkpoints.topic", "checkpoints.internal");
        _intervalMs = int.Parse(GetConfig(config, "checkpoints.interval.ms", "60000"));
        _syncGroupOffsets = bool.Parse(GetConfig(config, "sync.group.offsets.enabled", "true"));

        _policy = ReplicationPolicyFactory.Create(
            GetConfig(config, "replication.policy.class", "default"),
            GetConfig(config, "replication.policy.separator", "."));

        _groupFilter = new ConsumerGroupFilter(
            GetConfig(config, "groups", ".*"),
            ParseList(GetConfig(config, "groups.whitelist", "")),
            ParseList(GetConfig(config, "groups.blacklist", "")));

        _offsetTranslator = new OffsetTranslator();
        _checkpointStore = new CheckpointStore();

        // Connect to source cluster if provided
        var sourceBootstrap = GetConfig(config, "source.bootstrap.servers", "");
        if (!string.IsNullOrEmpty(sourceBootstrap))
        {
            var (host, port) = ParseBootstrapServers(sourceBootstrap);
            _sourceClient = new SurgewaveNativeClient(host, port);
            _sourceClient.ConnectAsync().GetAwaiter().GetResult();
        }
    }

    public override void Stop()
    {
        _sourceClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (!_syncGroupOffsets)
            return [];

        var now = DateTime.UtcNow;
        var elapsed = (now - _lastCheckpoint).TotalMilliseconds;

        if (elapsed < _intervalMs)
        {
            var delay = (int)(_intervalMs - elapsed);
            await Task.Delay(delay, cancellationToken);
        }

        _lastCheckpoint = DateTime.UtcNow;

        var records = new List<SourceRecord>();
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Get all checkpoint data from the store
        foreach (var checkpoint in _checkpointStore.All)
        {
            if (!_groupFilter.ShouldSync(checkpoint.ConsumerGroup))
                continue;

            var checkpointRecord = new CheckpointRecord
            {
                ConsumerGroup = checkpoint.ConsumerGroup,
                Topic = checkpoint.Topic,
                Partition = checkpoint.Partition,
                SourceOffset = checkpoint.SourceOffset,
                TargetOffset = checkpoint.TargetOffset,
                Metadata = $"{_sourceClusterAlias}->{_targetClusterAlias}",
                Timestamp = timestamp
            };

            var key = Encoding.UTF8.GetBytes(
                $"{checkpoint.ConsumerGroup}:{checkpoint.Topic}:{checkpoint.Partition}");
            var value = JsonSerializer.SerializeToUtf8Bytes(checkpointRecord);

            var record = new SourceRecord
            {
                SourcePartition = new Dictionary<string, object>
                {
                    ["cluster"] = _sourceClusterAlias,
                    ["group"] = checkpoint.ConsumerGroup
                },
                SourceOffset = new Dictionary<string, object>
                {
                    ["timestamp"] = timestamp
                },
                Topic = _policy.CheckpointTopic(_sourceClusterAlias, _targetClusterAlias),
                Key = key,
                Value = value,
                Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(timestamp),
                Headers = null
            };

            records.Add(record);
        }

        return records;
    }

    /// <summary>
    /// Update the checkpoint store with new offset mapping.
    /// Called by the replication task when records are committed.
    /// </summary>
    public void UpdateCheckpoint(string consumerGroup, string topic, int partition,
        long sourceOffset, long targetOffset)
    {
        var checkpoint = new Checkpoint
        {
            ConsumerGroup = consumerGroup,
            Topic = topic,
            Partition = partition,
            SourceOffset = sourceOffset,
            TargetOffset = targetOffset,
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        _checkpointStore.Store(checkpoint);
        _offsetTranslator.StoreMapping(_sourceClusterAlias, topic, partition, sourceOffset, targetOffset);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _sourceClient?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        base.Dispose(disposing);
    }

    private static (string host, int port) ParseBootstrapServers(string servers)
    {
        var parts = servers.Split(':');
        return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 9092);
    }

    private static string GetConfig(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;

    private static string[] ParseList(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return [];
        return value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }
}
