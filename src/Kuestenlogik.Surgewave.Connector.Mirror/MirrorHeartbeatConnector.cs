using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mirror;

/// <summary>
/// Connector that emits heartbeat records to monitor cluster replication health.
/// Heartbeats are used to detect replication lag and cluster connectivity issues.
/// </summary>
public sealed class MirrorHeartbeatConnector : SourceConnector
{
    private MirrorMakerConfig _config = null!;

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(MirrorHeartbeatTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("source.cluster.alias", ConfigType.String, Importance.High,
            "Alias for the source cluster")
        .Define("target.cluster.alias", ConfigType.String, Importance.High,
            "Alias for the target cluster")
        .Define("heartbeats.topic", ConfigType.String, "heartbeats", Importance.Medium,
            "Topic name for heartbeat records", EditorHint.Topic)
        .Define("heartbeats.interval.ms", ConfigType.Int, 1000L, Importance.Medium,
            "Interval between heartbeat emissions in milliseconds")
        .Define("replication.policy.class", ConfigType.String,
            "Kuestenlogik.Surgewave.Connect.Mirror.Policies.DefaultReplicationPolicy", Importance.Low,
            "Replication policy class for topic naming")
        .Define("replication.policy.separator", ConfigType.String, ".", Importance.Low,
            "Separator for topic naming in replication policy");

    public override void Start(IDictionary<string, string> config)
    {
        _config = MirrorMakerConfig.FromDictionary(config);

        if (string.IsNullOrEmpty(_config.SourceClusterAlias))
            throw new ArgumentException("source.cluster.alias is required");
    }

    public override void Stop()
    {
        // Nothing to clean up
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Only one heartbeat task is needed
        var taskConfig = new Dictionary<string, string>
        {
            ["source.cluster.alias"] = _config.SourceClusterAlias,
            ["target.cluster.alias"] = _config.TargetClusterAlias,
            ["heartbeats.topic"] = _config.HeartbeatsTopic,
            ["heartbeats.interval.ms"] = _config.HeartbeatIntervalMs.ToString(),
            ["replication.policy.class"] = _config.ReplicationPolicyClass,
            ["replication.policy.separator"] = _config.ReplicationPolicySeparator
        };

        return [taskConfig];
    }
}
