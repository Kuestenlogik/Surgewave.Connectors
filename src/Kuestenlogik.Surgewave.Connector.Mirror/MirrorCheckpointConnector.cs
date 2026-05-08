using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mirror;

/// <summary>
/// Connector that emits checkpoint records for consumer group offset synchronization.
/// Checkpoints map source cluster offsets to target cluster offsets for failover support.
/// </summary>
public sealed class MirrorCheckpointConnector : SourceConnector
{
    private MirrorMakerConfig _config = null!;

    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(MirrorCheckpointTask);

    public override ConfigDef Config => new ConfigDef()
        .Define("source.cluster.alias", ConfigType.String, Importance.High,
            "Alias for the source cluster")
        .Define("target.cluster.alias", ConfigType.String, Importance.High,
            "Alias for the target cluster")
        .Define("source.bootstrap.servers", ConfigType.String, Importance.High,
            "Bootstrap servers for source cluster")
        .Define("target.bootstrap.servers", ConfigType.String, Importance.High,
            "Bootstrap servers for target cluster")
        .Define("checkpoints.topic", ConfigType.String, "checkpoints.internal", Importance.Medium,
            "Topic name for checkpoint records", EditorHint.Topic)
        .Define("checkpoints.interval.ms", ConfigType.Int, 60000L, Importance.Medium,
            "Interval between checkpoint emissions in milliseconds")
        .Define("groups", ConfigType.String, ".*", Importance.Medium,
            "Regex pattern for consumer groups to sync")
        .Define("groups.whitelist", ConfigType.String, "", Importance.Medium,
            "Comma-separated list of consumer groups to sync")
        .Define("groups.blacklist", ConfigType.String, "", Importance.Medium,
            "Comma-separated list of consumer groups to exclude")
        .Define("sync.group.offsets.enabled", ConfigType.Boolean, true, Importance.Medium,
            "Enable consumer group offset synchronization")
        .Define("replication.policy.class", ConfigType.String,
            "Kuestenlogik.Surgewave.Connect.Mirror.Policies.DefaultReplicationPolicy", Importance.Low,
            "Replication policy class for topic naming")
        .Define("replication.policy.separator", ConfigType.String, ".", Importance.Low,
            "Separator for topic naming in replication policy");

    public override void Start(IDictionary<string, string> config)
    {
        _config = MirrorMakerConfig.FromDictionary(config);

        if (string.IsNullOrEmpty(_config.SourceBootstrapServers))
            throw new ArgumentException("source.bootstrap.servers is required");
    }

    public override void Stop()
    {
        // Nothing to clean up
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Only one checkpoint task is needed
        var taskConfig = new Dictionary<string, string>
        {
            ["source.cluster.alias"] = _config.SourceClusterAlias,
            ["target.cluster.alias"] = _config.TargetClusterAlias,
            ["source.bootstrap.servers"] = _config.SourceBootstrapServers,
            ["target.bootstrap.servers"] = _config.TargetBootstrapServers,
            ["checkpoints.topic"] = _config.CheckpointsTopic,
            ["checkpoints.interval.ms"] = _config.CheckpointIntervalMs.ToString(),
            ["groups"] = _config.GroupsPattern,
            ["groups.whitelist"] = string.Join(",", _config.GroupsWhitelist),
            ["groups.blacklist"] = string.Join(",", _config.GroupsBlacklist),
            ["sync.group.offsets.enabled"] = _config.SyncGroupOffsets.ToString(),
            ["replication.policy.class"] = _config.ReplicationPolicyClass,
            ["replication.policy.separator"] = _config.ReplicationPolicySeparator
        };

        return [taskConfig];
    }
}
