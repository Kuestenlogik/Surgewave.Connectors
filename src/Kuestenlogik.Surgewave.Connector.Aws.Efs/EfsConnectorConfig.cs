namespace Kuestenlogik.Surgewave.Connector.Aws.Efs;

/// <summary>
/// Configuration constants for AWS EFS connectors.
/// </summary>
public static class EfsConnectorConfig
{
    // Connection
    public const string RegionConfig = "aws.region";
    public const string AccessKeyConfig = "aws.access.key.id";
    public const string SecretKeyConfig = "aws.secret.access.key";
    public const string EndpointConfig = "aws.endpoint";

    // Source
    public const string TopicConfig = "topic";
    public const string PollIntervalMsConfig = "poll.interval.ms";
    public const string FileSystemIdsConfig = "file.system.ids";
    public const string IncludeMountTargetsConfig = "include.mount.targets";
    public const string IncludeAccessPointsConfig = "include.access.points";

    // Sink
    public const string OperationFieldConfig = "operation.field";
    public const string FileSystemIdFieldConfig = "file.system.id.field";
    public const string NameFieldConfig = "name.field";
    public const string PerformanceModeConfig = "performance.mode";
    public const string ThroughputModeConfig = "throughput.mode";
    public const string ProvisionedThroughputConfig = "provisioned.throughput.mibps";
    public const string EncryptedConfig = "encrypted";
    public const string KmsKeyIdConfig = "kms.key.id";
    public const string DefaultTagsConfig = "default.tags";

    // Access Point Sink
    public const string AccessPointPathConfig = "access.point.path";
    public const string PosixUserUidConfig = "posix.user.uid";
    public const string PosixUserGidConfig = "posix.user.gid";
    public const string RootDirectoryPermissionsConfig = "root.directory.permissions";

    // Defaults
    public const string DefaultRegion = "us-east-1";
    public const int DefaultPollIntervalMs = 30000;
    public const string DefaultPerformanceMode = "generalPurpose";
    public const string DefaultThroughputMode = "bursting";
    public const string DefaultOperationField = "operation";
    public const string DefaultFileSystemIdField = "file_system_id";
    public const string DefaultNameField = "name";

    // Operations
    public const string OperationCreateFileSystem = "create_file_system";
    public const string OperationDeleteFileSystem = "delete_file_system";
    public const string OperationUpdateFileSystem = "update_file_system";
    public const string OperationCreateAccessPoint = "create_access_point";
    public const string OperationDeleteAccessPoint = "delete_access_point";
    public const string OperationCreateMountTarget = "create_mount_target";
    public const string OperationDeleteMountTarget = "delete_mount_target";

    // Headers
    public const string HeaderFileSystemId = "efs.file.system.id";
    public const string HeaderOperation = "efs.operation";
    public const string HeaderLifeCycleState = "efs.lifecycle.state";
}
