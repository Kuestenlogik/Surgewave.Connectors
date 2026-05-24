using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.ElasticFileSystem;
using Amazon.ElasticFileSystem.Model;
using Amazon.Runtime;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Efs;

/// <summary>
/// Sink task that manages AWS EFS resources.
/// Supports creating, updating, and deleting file systems, access points, and mount targets.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class EfsSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true
    };

    public override string Version => "1.0.0";

    private AmazonElasticFileSystemClient? _efsClient;
    private string _operationField = EfsConnectorConfig.DefaultOperationField;
    private string _fileSystemIdField = EfsConnectorConfig.DefaultFileSystemIdField;
    private string _nameField = EfsConnectorConfig.DefaultNameField;
    private string _defaultPerformanceMode = EfsConnectorConfig.DefaultPerformanceMode;
    private string _defaultThroughputMode = EfsConnectorConfig.DefaultThroughputMode;
    private double _defaultProvisionedThroughput;
    private bool _defaultEncrypted = true;
    private string? _defaultKmsKeyId;
    private Dictionary<string, string> _defaultTags = new();

    public override void Start(IDictionary<string, string> config)
    {
        var region = GetConfigValue(config, EfsConnectorConfig.RegionConfig, EfsConnectorConfig.DefaultRegion);
        _operationField = GetConfigValue(config, EfsConnectorConfig.OperationFieldConfig, EfsConnectorConfig.DefaultOperationField);
        _fileSystemIdField = GetConfigValue(config, EfsConnectorConfig.FileSystemIdFieldConfig, EfsConnectorConfig.DefaultFileSystemIdField);
        _nameField = GetConfigValue(config, EfsConnectorConfig.NameFieldConfig, EfsConnectorConfig.DefaultNameField);
        _defaultPerformanceMode = GetConfigValue(config, EfsConnectorConfig.PerformanceModeConfig, EfsConnectorConfig.DefaultPerformanceMode);
        _defaultThroughputMode = GetConfigValue(config, EfsConnectorConfig.ThroughputModeConfig, EfsConnectorConfig.DefaultThroughputMode);
        _defaultProvisionedThroughput = double.Parse(GetConfigValue(config, EfsConnectorConfig.ProvisionedThroughputConfig, "0"));
        _defaultEncrypted = bool.Parse(GetConfigValue(config, EfsConnectorConfig.EncryptedConfig, "true"));
        _defaultKmsKeyId = GetConfigValue(config, EfsConnectorConfig.KmsKeyIdConfig, "");

        var tagsStr = GetConfigValue(config, EfsConnectorConfig.DefaultTagsConfig, "");
        if (!string.IsNullOrWhiteSpace(tagsStr))
        {
            foreach (var pair in tagsStr.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                var parts = pair.Split('=', 2);
                if (parts.Length == 2)
                {
                    _defaultTags[parts[0].Trim()] = parts[1].Trim();
                }
            }
        }

        var clientConfig = new AmazonElasticFileSystemConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(region)
        };

        var endpoint = GetConfigValue(config, EfsConnectorConfig.EndpointConfig, "");
        if (!string.IsNullOrEmpty(endpoint))
        {
            clientConfig.ServiceURL = endpoint;
        }

        var accessKey = GetConfigValue(config, EfsConnectorConfig.AccessKeyConfig, "");
        var secretKey = GetConfigValue(config, EfsConnectorConfig.SecretKeyConfig, "");

        if (!string.IsNullOrEmpty(accessKey) && !string.IsNullOrEmpty(secretKey))
        {
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            _efsClient = new AmazonElasticFileSystemClient(credentials, clientConfig);
        }
        else
        {
            _efsClient = new AmazonElasticFileSystemClient(clientConfig);
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_efsClient == null) return;

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                var json = Encoding.UTF8.GetString(record.Value);
                var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json, JsonOptions);
                if (data == null) continue;

                var operation = data.TryGetValue(_operationField, out var opEl) ? opEl.GetString() : null;
                if (string.IsNullOrEmpty(operation)) continue;

                await ProcessOperationAsync(operation, data, cancellationToken);
            }
            catch (Exception)
            {
                // Log and continue
            }
        }
    }

    private async Task ProcessOperationAsync(string operation, Dictionary<string, JsonElement> data, CancellationToken cancellationToken)
    {
        switch (operation.ToLowerInvariant())
        {
            case EfsConnectorConfig.OperationCreateFileSystem:
                await CreateFileSystemAsync(data, cancellationToken);
                break;

            case EfsConnectorConfig.OperationDeleteFileSystem:
                await DeleteFileSystemAsync(data, cancellationToken);
                break;

            case EfsConnectorConfig.OperationUpdateFileSystem:
                await UpdateFileSystemAsync(data, cancellationToken);
                break;

            case EfsConnectorConfig.OperationCreateAccessPoint:
                await CreateAccessPointAsync(data, cancellationToken);
                break;

            case EfsConnectorConfig.OperationDeleteAccessPoint:
                await DeleteAccessPointAsync(data, cancellationToken);
                break;

            case EfsConnectorConfig.OperationCreateMountTarget:
                await CreateMountTargetAsync(data, cancellationToken);
                break;

            case EfsConnectorConfig.OperationDeleteMountTarget:
                await DeleteMountTargetAsync(data, cancellationToken);
                break;
        }
    }

    private async Task CreateFileSystemAsync(Dictionary<string, JsonElement> data, CancellationToken cancellationToken)
    {
        var request = new CreateFileSystemRequest
        {
            CreationToken = Guid.NewGuid().ToString(),
            PerformanceMode = GetStringValue(data, "performance_mode", _defaultPerformanceMode),
            ThroughputMode = GetStringValue(data, "throughput_mode", _defaultThroughputMode),
            Encrypted = GetBoolValue(data, "encrypted", _defaultEncrypted)
        };

        var throughputMode = request.ThroughputMode;
        if (throughputMode == "provisioned" || throughputMode == ThroughputMode.Provisioned)
        {
            request.ProvisionedThroughputInMibps = GetDoubleValue(data, "provisioned_throughput_mibps", _defaultProvisionedThroughput);
        }

        var kmsKeyId = GetStringValue(data, "kms_key_id", _defaultKmsKeyId ?? "");
        if (!string.IsNullOrEmpty(kmsKeyId))
        {
            request.KmsKeyId = kmsKeyId;
        }

        // Add tags
        var tags = new List<Tag>();
        foreach (var kvp in _defaultTags)
        {
            tags.Add(new Tag { Key = kvp.Key, Value = kvp.Value });
        }

        var name = GetStringValue(data, _nameField, "");
        if (!string.IsNullOrEmpty(name))
        {
            tags.Add(new Tag { Key = "Name", Value = name });
        }

        if (tags.Count > 0)
        {
            request.Tags = tags;
        }

        await _efsClient!.CreateFileSystemAsync(request, cancellationToken);
    }

    private async Task DeleteFileSystemAsync(Dictionary<string, JsonElement> data, CancellationToken cancellationToken)
    {
        var fileSystemId = GetStringValue(data, _fileSystemIdField, "");
        if (string.IsNullOrEmpty(fileSystemId)) return;

        var request = new DeleteFileSystemRequest { FileSystemId = fileSystemId };
        await _efsClient!.DeleteFileSystemAsync(request, cancellationToken);
    }

    private async Task UpdateFileSystemAsync(Dictionary<string, JsonElement> data, CancellationToken cancellationToken)
    {
        var fileSystemId = GetStringValue(data, _fileSystemIdField, "");
        if (string.IsNullOrEmpty(fileSystemId)) return;

        var request = new UpdateFileSystemRequest { FileSystemId = fileSystemId };

        if (data.TryGetValue("throughput_mode", out var tmEl))
        {
            request.ThroughputMode = tmEl.GetString();
        }

        if (data.TryGetValue("provisioned_throughput_mibps", out var ptEl))
        {
            request.ProvisionedThroughputInMibps = ptEl.GetDouble();
        }

        await _efsClient!.UpdateFileSystemAsync(request, cancellationToken);
    }

    private async Task CreateAccessPointAsync(Dictionary<string, JsonElement> data, CancellationToken cancellationToken)
    {
        var fileSystemId = GetStringValue(data, _fileSystemIdField, "");
        if (string.IsNullOrEmpty(fileSystemId)) return;

        var request = new CreateAccessPointRequest
        {
            FileSystemId = fileSystemId,
            ClientToken = Guid.NewGuid().ToString()
        };

        var path = GetStringValue(data, "path", "");
        if (!string.IsNullOrEmpty(path))
        {
            request.RootDirectory = new RootDirectory
            {
                Path = path,
                CreationInfo = new CreationInfo
                {
                    OwnerUid = GetLongValue(data, "owner_uid", 1000),
                    OwnerGid = GetLongValue(data, "owner_gid", 1000),
                    Permissions = GetStringValue(data, "permissions", "755")
                }
            };
        }

        if (data.ContainsKey("posix_uid") || data.ContainsKey("posix_gid"))
        {
            request.PosixUser = new PosixUser
            {
                Uid = GetLongValue(data, "posix_uid", 1000),
                Gid = GetLongValue(data, "posix_gid", 1000)
            };
        }

        var name = GetStringValue(data, _nameField, "");
        if (!string.IsNullOrEmpty(name))
        {
            request.Tags = [new Tag { Key = "Name", Value = name }];
        }

        await _efsClient!.CreateAccessPointAsync(request, cancellationToken);
    }

    private async Task DeleteAccessPointAsync(Dictionary<string, JsonElement> data, CancellationToken cancellationToken)
    {
        var accessPointId = GetStringValue(data, "access_point_id", "");
        if (string.IsNullOrEmpty(accessPointId)) return;

        var request = new DeleteAccessPointRequest { AccessPointId = accessPointId };
        await _efsClient!.DeleteAccessPointAsync(request, cancellationToken);
    }

    private async Task CreateMountTargetAsync(Dictionary<string, JsonElement> data, CancellationToken cancellationToken)
    {
        var fileSystemId = GetStringValue(data, _fileSystemIdField, "");
        var subnetId = GetStringValue(data, "subnet_id", "");
        if (string.IsNullOrEmpty(fileSystemId) || string.IsNullOrEmpty(subnetId)) return;

        var request = new CreateMountTargetRequest
        {
            FileSystemId = fileSystemId,
            SubnetId = subnetId
        };

        var ipAddress = GetStringValue(data, "ip_address", "");
        if (!string.IsNullOrEmpty(ipAddress))
        {
            request.IpAddress = ipAddress;
        }

        if (data.TryGetValue("security_groups", out var sgEl) && sgEl.ValueKind == JsonValueKind.Array)
        {
            request.SecurityGroups = sgEl.EnumerateArray()
                .Select(e => e.GetString()!)
                .Where(s => !string.IsNullOrEmpty(s))
                .ToList();
        }

        await _efsClient!.CreateMountTargetAsync(request, cancellationToken);
    }

    private async Task DeleteMountTargetAsync(Dictionary<string, JsonElement> data, CancellationToken cancellationToken)
    {
        var mountTargetId = GetStringValue(data, "mount_target_id", "");
        if (string.IsNullOrEmpty(mountTargetId)) return;

        var request = new DeleteMountTargetRequest { MountTargetId = mountTargetId };
        await _efsClient!.DeleteMountTargetAsync(request, cancellationToken);
    }

    private static string GetStringValue(Dictionary<string, JsonElement> data, string key, string defaultValue)
    {
        if (data.TryGetValue(key, out var el) && el.ValueKind == JsonValueKind.String)
            return el.GetString() ?? defaultValue;
        return defaultValue;
    }

    private static bool GetBoolValue(Dictionary<string, JsonElement> data, string key, bool defaultValue)
    {
        if (data.TryGetValue(key, out var el))
        {
            if (el.ValueKind == JsonValueKind.True) return true;
            if (el.ValueKind == JsonValueKind.False) return false;
            if (el.ValueKind == JsonValueKind.String && bool.TryParse(el.GetString(), out var b)) return b;
        }
        return defaultValue;
    }

    private static double GetDoubleValue(Dictionary<string, JsonElement> data, string key, double defaultValue)
    {
        if (data.TryGetValue(key, out var el))
        {
            if (el.ValueKind == JsonValueKind.Number) return el.GetDouble();
            if (el.ValueKind == JsonValueKind.String && double.TryParse(el.GetString(), out var d)) return d;
        }
        return defaultValue;
    }

    private static long GetLongValue(Dictionary<string, JsonElement> data, string key, long defaultValue)
    {
        if (data.TryGetValue(key, out var el))
        {
            if (el.ValueKind == JsonValueKind.Number) return el.GetInt64();
            if (el.ValueKind == JsonValueKind.String && long.TryParse(el.GetString(), out var l)) return l;
        }
        return defaultValue;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
        _efsClient?.Dispose();
        _efsClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Stop();
        base.Dispose(disposing);
    }
}
