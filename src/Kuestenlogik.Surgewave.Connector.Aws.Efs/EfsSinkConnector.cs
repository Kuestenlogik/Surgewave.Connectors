using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Aws.Efs;

/// <summary>
/// Sink connector that manages AWS EFS resources (file systems, access points, mount targets).
/// Processes operations to create, update, and delete EFS resources.
/// </summary>
[ConnectorMetadata(
    Name = "efs-sink",
    Description = "Manages AWS EFS resources via management API",
    Author = "Surgewave",
    Tags = "aws,efs,storage,cloud,file-system")]
public sealed class EfsSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(EfsSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config { get; } = new ConfigDef()
        .Define(EfsConnectorConfig.RegionConfig, ConfigType.String, EfsConnectorConfig.DefaultRegion, Importance.Medium, "AWS region")
        .Define(EfsConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS access key ID (optional, uses default credential chain)")
        .Define(EfsConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium, "AWS secret access key")
        .Define(EfsConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low, "Custom endpoint URL (e.g., for LocalStack)")
        .Define(EfsConnectorConfig.OperationFieldConfig, ConfigType.String, EfsConnectorConfig.DefaultOperationField, Importance.Medium, "Field containing the operation type")
        .Define(EfsConnectorConfig.FileSystemIdFieldConfig, ConfigType.String, EfsConnectorConfig.DefaultFileSystemIdField, Importance.Medium, "Field containing the file system ID")
        .Define(EfsConnectorConfig.NameFieldConfig, ConfigType.String, EfsConnectorConfig.DefaultNameField, Importance.Low, "Field containing the file system name")
        .Define(EfsConnectorConfig.PerformanceModeConfig, ConfigType.String, EfsConnectorConfig.DefaultPerformanceMode, Importance.Low, "Default performance mode (generalPurpose, maxIO)", EditorHint.Select, options: ["generalPurpose", "maxIO"])
        .Define(EfsConnectorConfig.ThroughputModeConfig, ConfigType.String, EfsConnectorConfig.DefaultThroughputMode, Importance.Low, "Default throughput mode (bursting, provisioned, elastic)", EditorHint.Select, options: ["bursting", "provisioned", "elastic"])
        .Define(EfsConnectorConfig.ProvisionedThroughputConfig, ConfigType.Double, 0.0, Importance.Low, "Provisioned throughput in MiB/s (required for provisioned mode)")
        .Define(EfsConnectorConfig.EncryptedConfig, ConfigType.Boolean, true, Importance.Low, "Enable encryption at rest")
        .Define(EfsConnectorConfig.KmsKeyIdConfig, ConfigType.String, "", Importance.Low, "KMS key ID for encryption (uses AWS managed key if empty)")
        .Define(EfsConnectorConfig.DefaultTagsConfig, ConfigType.String, "", Importance.Low, "Default tags in format key1=value1,key2=value2", EditorHint.Multiline);

    public override void Start(IDictionary<string, string> config)
    {
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
