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
/// Source task that polls AWS EFS for file system status and metadata.
/// Produces events when file systems, mount targets, or access points change.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class EfsSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private AmazonElasticFileSystemClient? _efsClient;
    private string _topic = "";
    private string _region = EfsConnectorConfig.DefaultRegion;
    private HashSet<string>? _fileSystemIds;
    private int _pollIntervalMs = EfsConnectorConfig.DefaultPollIntervalMs;
    private bool _includeMountTargets = true;
    private bool _includeAccessPoints = true;
    private long _messageId;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;

    private readonly Dictionary<string, FileSystemState> _lastKnownStates = new();

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[EfsConnectorConfig.TopicConfig];
        _region = GetConfigValue(config, EfsConnectorConfig.RegionConfig, EfsConnectorConfig.DefaultRegion);
        _pollIntervalMs = int.Parse(GetConfigValue(config, EfsConnectorConfig.PollIntervalMsConfig, EfsConnectorConfig.DefaultPollIntervalMs.ToString()));
        _includeMountTargets = bool.Parse(GetConfigValue(config, EfsConnectorConfig.IncludeMountTargetsConfig, "true"));
        _includeAccessPoints = bool.Parse(GetConfigValue(config, EfsConnectorConfig.IncludeAccessPointsConfig, "true"));

        var fileSystemIdsStr = GetConfigValue(config, EfsConnectorConfig.FileSystemIdsConfig, "");
        if (!string.IsNullOrWhiteSpace(fileSystemIdsStr))
        {
            _fileSystemIds = fileSystemIdsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
        }

        var clientConfig = new AmazonElasticFileSystemConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(_region)
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

    public override void Stop()
    {
        _efsClient?.Dispose();
        _efsClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var records = new List<SourceRecord>();

        var elapsed = (DateTimeOffset.UtcNow - _lastPollTime).TotalMilliseconds;
        if (elapsed < _pollIntervalMs)
        {
            await Task.Delay((int)(_pollIntervalMs - elapsed), cancellationToken);
        }
        _lastPollTime = DateTimeOffset.UtcNow;

        if (_efsClient == null)
            return records;

        try
        {
            var fileSystems = await GetFileSystemsAsync(cancellationToken);

            foreach (var fs in fileSystems)
            {
                if (_fileSystemIds != null && !_fileSystemIds.Contains(fs.FileSystemId))
                    continue;

                var currentState = await BuildFileSystemStateAsync(fs, cancellationToken);

                if (!_lastKnownStates.TryGetValue(fs.FileSystemId, out var lastState) ||
                    !currentState.Equals(lastState))
                {
                    records.Add(CreateSourceRecord(fs, currentState));
                    _lastKnownStates[fs.FileSystemId] = currentState;
                }
            }
        }
        catch (OperationCanceledException)
        {
        }

        return records;
    }

    private async Task<List<FileSystemDescription>> GetFileSystemsAsync(CancellationToken cancellationToken)
    {
        var fileSystems = new List<FileSystemDescription>();
        string? marker = null;

        do
        {
            var request = new DescribeFileSystemsRequest { Marker = marker };
            var response = await _efsClient!.DescribeFileSystemsAsync(request, cancellationToken);
            fileSystems.AddRange(response.FileSystems);
            marker = response.NextMarker;
        } while (!string.IsNullOrEmpty(marker));

        return fileSystems;
    }

    private async Task<FileSystemState> BuildFileSystemStateAsync(FileSystemDescription fs, CancellationToken cancellationToken)
    {
        var state = new FileSystemState
        {
            FileSystemId = fs.FileSystemId,
            LifeCycleState = fs.LifeCycleState.Value,
            SizeInBytes = fs.SizeInBytes?.Value ?? 0,
            NumberOfMountTargets = fs.NumberOfMountTargets ?? 0,
            PerformanceMode = fs.PerformanceMode.Value,
            ThroughputMode = fs.ThroughputMode.Value,
            ProvisionedThroughputInMibps = fs.ProvisionedThroughputInMibps ?? 0
        };

        if (_includeMountTargets)
        {
            var mtRequest = new DescribeMountTargetsRequest { FileSystemId = fs.FileSystemId };
            var mtResponse = await _efsClient!.DescribeMountTargetsAsync(mtRequest, cancellationToken);
            state.MountTargetStates = mtResponse.MountTargets
                .Select(mt => $"{mt.MountTargetId}:{mt.LifeCycleState}")
                .OrderBy(s => s)
                .ToList();
        }

        if (_includeAccessPoints)
        {
            var apRequest = new DescribeAccessPointsRequest { FileSystemId = fs.FileSystemId };
            var apResponse = await _efsClient!.DescribeAccessPointsAsync(apRequest, cancellationToken);
            state.AccessPointStates = apResponse.AccessPoints
                .Select(ap => $"{ap.AccessPointId}:{ap.LifeCycleState}")
                .OrderBy(s => s)
                .ToList();
        }

        return state;
    }

    private SourceRecord CreateSourceRecord(FileSystemDescription fs, FileSystemState state)
    {
        var eventData = new Dictionary<string, object?>
        {
            ["file_system_id"] = fs.FileSystemId,
            ["file_system_arn"] = fs.FileSystemArn,
            ["name"] = fs.Name,
            ["creation_time"] = fs.CreationTime,
            ["lifecycle_state"] = fs.LifeCycleState.Value,
            ["size_in_bytes"] = fs.SizeInBytes.Value,
            ["number_of_mount_targets"] = fs.NumberOfMountTargets,
            ["performance_mode"] = fs.PerformanceMode.Value,
            ["throughput_mode"] = fs.ThroughputMode.Value,
            ["provisioned_throughput_mibps"] = fs.ProvisionedThroughputInMibps,
            ["encrypted"] = fs.Encrypted,
            ["kms_key_id"] = fs.KmsKeyId,
            ["tags"] = fs.Tags?.ToDictionary(t => t.Key, t => t.Value),
            ["mount_targets"] = state.MountTargetStates,
            ["access_points"] = state.AccessPointStates
        };

        var json = JsonSerializer.Serialize(eventData);
        var value = Encoding.UTF8.GetBytes(json);
        var key = Encoding.UTF8.GetBytes(fs.FileSystemId);

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object>
            {
                ["region"] = _region
            },
            SourceOffset = new Dictionary<string, object>
            {
                ["file_system_id"] = fs.FileSystemId,
                ["offset"] = Interlocked.Increment(ref _messageId)
            },
            Topic = _topic,
            Key = key,
            Value = value,
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>
            {
                [EfsConnectorConfig.HeaderFileSystemId] = Encoding.UTF8.GetBytes(fs.FileSystemId),
                [EfsConnectorConfig.HeaderLifeCycleState] = Encoding.UTF8.GetBytes(fs.LifeCycleState.Value)
            }
        };
    }

    private sealed class FileSystemState : IEquatable<FileSystemState>
    {
        public string FileSystemId { get; set; } = "";
        public string LifeCycleState { get; set; } = "";
        public long SizeInBytes { get; set; }
        public int NumberOfMountTargets { get; set; }
        public string PerformanceMode { get; set; } = "";
        public string ThroughputMode { get; set; } = "";
        public double ProvisionedThroughputInMibps { get; set; }
        public List<string> MountTargetStates { get; set; } = [];
        public List<string> AccessPointStates { get; set; } = [];

        public bool Equals(FileSystemState? other)
        {
            if (other == null) return false;
            return FileSystemId == other.FileSystemId &&
                   LifeCycleState == other.LifeCycleState &&
                   SizeInBytes == other.SizeInBytes &&
                   NumberOfMountTargets == other.NumberOfMountTargets &&
                   PerformanceMode == other.PerformanceMode &&
                   ThroughputMode == other.ThroughputMode &&
                   Math.Abs(ProvisionedThroughputInMibps - other.ProvisionedThroughputInMibps) < 0.001 &&
                   MountTargetStates.SequenceEqual(other.MountTargetStates) &&
                   AccessPointStates.SequenceEqual(other.AccessPointStates);
        }

        public override bool Equals(object? obj) => Equals(obj as FileSystemState);
        public override int GetHashCode() => HashCode.Combine(FileSystemId, LifeCycleState);
    }
}
