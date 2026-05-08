using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connector.Mirror.Models;
using Kuestenlogik.Surgewave.Connector.Mirror.Policies;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mirror;

/// <summary>
/// Task that emits heartbeat records to a dedicated topic.
/// Heartbeats contain timestamp and source cluster information for lag monitoring.
/// </summary>
public sealed class MirrorHeartbeatTask : SourceTask
{
    private string _sourceClusterAlias = "";
    private string _targetClusterAlias = "";
    private string _heartbeatsTopic = "";
    private int _intervalMs;
    private IReplicationPolicy _policy = null!;
    private DateTime _lastHeartbeat = DateTime.MinValue;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _sourceClusterAlias = GetConfig(config, "source.cluster.alias", "source");
        _targetClusterAlias = GetConfig(config, "target.cluster.alias", "target");
        _heartbeatsTopic = GetConfig(config, "heartbeats.topic", "heartbeats");
        _intervalMs = int.Parse(GetConfig(config, "heartbeats.interval.ms", "1000"));

        _policy = ReplicationPolicyFactory.Create(
            GetConfig(config, "replication.policy.class", "default"),
            GetConfig(config, "replication.policy.separator", "."));
    }

    public override void Stop()
    {
        // Nothing to clean up
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        var elapsed = (now - _lastHeartbeat).TotalMilliseconds;

        if (elapsed < _intervalMs)
        {
            var delay = (int)(_intervalMs - elapsed);
            await Task.Delay(delay, cancellationToken);
        }

        _lastHeartbeat = DateTime.UtcNow;

        var heartbeat = new Heartbeat
        {
            SourceCluster = _sourceClusterAlias,
            TargetCluster = _targetClusterAlias,
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        var key = Encoding.UTF8.GetBytes($"{_sourceClusterAlias}->{_targetClusterAlias}");
        var value = JsonSerializer.SerializeToUtf8Bytes(heartbeat);

        var record = new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["cluster"] = _sourceClusterAlias
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["timestamp"] = heartbeat.Timestamp
            },
            Topic = _policy.HeartbeatTopic(_sourceClusterAlias),
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(heartbeat.Timestamp),
            Headers = null
        };

        return [record];
    }

    private static string GetConfig(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;
}
