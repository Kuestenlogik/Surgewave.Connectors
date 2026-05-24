using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Aws.Efs;

/// <summary>
/// Source connector that polls AWS EFS file systems for status and metadata changes.
/// Produces events for file system lifecycle state changes, mount targets, and access points.
/// </summary>
[ConnectorMetadata(
    Name = "efs-source",
    Description = "Polls AWS EFS file systems for status and metadata changes",
    Author = "Surgewave",
    Tags = "aws,efs,storage,cloud,file-system")]
public sealed class EfsSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(EfsSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(EfsConnectorConfig.TopicConfig, ConfigType.String, Importance.High, "Topic to produce EFS events to", EditorHint.Topic)
        .Define(EfsConnectorConfig.RegionConfig, ConfigType.String, EfsConnectorConfig.DefaultRegion, Importance.Medium, "AWS region")
        .Define(EfsConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS access key ID (optional, uses default credential chain)")
        .Define(EfsConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS secret access key")
        .Define(EfsConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (e.g., for LocalStack)")
        .Define(EfsConnectorConfig.FileSystemIdsConfig, ConfigType.String, "", Importance.Medium, "Comma-separated file system IDs to monitor (empty = all)")
        .Define(EfsConnectorConfig.PollIntervalMsConfig, ConfigType.Int, EfsConnectorConfig.DefaultPollIntervalMs, Importance.Low, "Poll interval in milliseconds")
        .Define(EfsConnectorConfig.IncludeMountTargetsConfig, ConfigType.Boolean, true, Importance.Low, "Include mount target information")
        .Define(EfsConnectorConfig.IncludeAccessPointsConfig, ConfigType.Boolean, true, Importance.Low, "Include access point information");

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.TryGetValue(EfsConnectorConfig.TopicConfig, out var topic) || string.IsNullOrEmpty(topic))
            throw new ArgumentException($"Required configuration '{EfsConnectorConfig.TopicConfig}' is missing");

        _config = new Dictionary<string, string>(config);
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
