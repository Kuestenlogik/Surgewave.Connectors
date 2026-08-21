using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.ElasticFileSystem;
using Amazon.ElasticFileSystem.Model;
using Amazon.Runtime;

namespace Kuestenlogik.Surgewave.Connector.Aws.Efs.Tests;

/// <summary>
/// Tests for <see cref="EfsSourceTask"/> driven through a scripted EFS client, so the polling,
/// pagination and change detection run without calling AWS.
/// </summary>
public class EfsSourceTaskTests
{
    private static Dictionary<string, string> Config(
        string fileSystemIds = "",
        string mountTargets = "true",
        string accessPoints = "true") =>
        new()
        {
            [EfsConnectorConfig.TopicConfig] = "efs-events",
            [EfsConnectorConfig.RegionConfig] = "eu-central-1",
            [EfsConnectorConfig.PollIntervalMsConfig] = "0",
            [EfsConnectorConfig.IncludeMountTargetsConfig] = mountTargets,
            [EfsConnectorConfig.IncludeAccessPointsConfig] = accessPoints,
            [EfsConnectorConfig.FileSystemIdsConfig] = fileSystemIds
        };

    private static FileSystemDescription FileSystem(string id, LifeCycleState? lifeCycleState = null) =>
        new()
        {
            FileSystemId = id,
            FileSystemArn = $"arn:aws:elasticfilesystem:eu-central-1:1:file-system/{id}",
            Name = "analytics",
            CreationTime = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            LifeCycleState = lifeCycleState ?? LifeCycleState.Available,
            SizeInBytes = new FileSystemSize { Value = 1024 },
            NumberOfMountTargets = 2,
            PerformanceMode = PerformanceMode.GeneralPurpose,
            ThroughputMode = ThroughputMode.Bursting,
            ProvisionedThroughputInMibps = 0,
            Encrypted = true,
            KmsKeyId = "kms-1",
            Tags = [new Tag { Key = "env", Value = "prod" }]
        };

    private static DescribeFileSystemsResponse Page(params FileSystemDescription[] fileSystems) =>
        new() { FileSystems = [.. fileSystems] };

    private static MountTargetDescription MountTarget(string id) =>
        new()
        {
            MountTargetId = id,
            FileSystemId = "fs-1",
            SubnetId = "subnet-1",
            LifeCycleState = LifeCycleState.Available
        };

    private static AccessPointDescription AccessPoint(string id) =>
        new()
        {
            AccessPointId = id,
            FileSystemId = "fs-1",
            LifeCycleState = LifeCycleState.Available
        };

    private static JsonElement PayloadOf(byte[] value) =>
        JsonDocument.Parse(Encoding.UTF8.GetString(value)).RootElement.Clone();

    [Fact]
    public async Task PollAsync_EmitsARecordForEveryFileSystem()
    {
        using var client = new ScriptedEfsClient();
        client.FileSystemPages.Enqueue(Page(FileSystem("fs-1")));
        using var task = new EfsSourceTask();
        task.StartWith(Config(), client);

        var records = await task.PollAsync(CancellationToken.None);

        var record = Assert.Single(records);
        Assert.Equal("efs-events", record.Topic);
        Assert.Equal("fs-1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("eu-central-1", record.SourcePartition["region"]);
        Assert.Equal("fs-1", record.SourceOffset["file_system_id"]);
        Assert.Equal("fs-1", Encoding.UTF8.GetString(record.Headers![EfsConnectorConfig.HeaderFileSystemId]));
        Assert.Equal(
            LifeCycleState.Available.Value,
            Encoding.UTF8.GetString(record.Headers[EfsConnectorConfig.HeaderLifeCycleState]));

        var payload = PayloadOf(record.Value);
        Assert.Equal("fs-1", payload.GetProperty("file_system_id").GetString());
        Assert.Equal("analytics", payload.GetProperty("name").GetString());
        Assert.Equal(1024L, payload.GetProperty("size_in_bytes").GetInt64());
        Assert.Equal("prod", payload.GetProperty("tags").GetProperty("env").GetString());
    }

    [Fact]
    public async Task PollAsync_IgnoresFileSystemsOutsideTheConfiguredFilter()
    {
        using var client = new ScriptedEfsClient();
        client.FileSystemPages.Enqueue(Page(FileSystem("fs-1"), FileSystem("fs-2")));
        using var task = new EfsSourceTask();
        task.StartWith(Config(fileSystemIds: "fs-2"), client);

        var records = await task.PollAsync(CancellationToken.None);

        var record = Assert.Single(records);
        Assert.Equal("fs-2", Encoding.UTF8.GetString(record.Key!));
    }

    [Fact]
    public async Task PollAsync_EmitsAgainOnlyAfterTheFileSystemChanged()
    {
        using var client = new ScriptedEfsClient();
        var fileSystem = FileSystem("fs-1");
        client.FileSystemPages.Enqueue(Page(fileSystem));
        client.FileSystemPages.Enqueue(Page(fileSystem));
        client.FileSystemPages.Enqueue(Page(fileSystem));
        using var task = new EfsSourceTask();
        task.StartWith(Config(mountTargets: "false", accessPoints: "false"), client);

        Assert.Single(await task.PollAsync(CancellationToken.None));
        Assert.Empty(await task.PollAsync(CancellationToken.None));

        fileSystem.LifeCycleState = LifeCycleState.Updating;

        Assert.Single(await task.PollAsync(CancellationToken.None));
    }

    [Fact]
    public async Task PollAsync_FollowsThePaginationMarkerForFileSystems()
    {
        using var client = new ScriptedEfsClient();
        client.FileSystemPages.Enqueue(new DescribeFileSystemsResponse
        {
            FileSystems = [FileSystem("fs-1")],
            NextMarker = "page-2"
        });
        client.FileSystemPages.Enqueue(Page(FileSystem("fs-2")));
        using var task = new EfsSourceTask();
        task.StartWith(Config(mountTargets: "false", accessPoints: "false"), client);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(2, records.Count);
        Assert.Equal(new string?[] { null, "page-2" }, client.FileSystemMarkers);
    }

    [Fact]
    public async Task PollAsync_FollowsThePaginationMarkerForMountTargets()
    {
        // A file system with many mount targets used to be reported from its first page only.
        using var client = new ScriptedEfsClient();
        client.FileSystemPages.Enqueue(Page(FileSystem("fs-1")));
        client.MountTargetPages.Enqueue(new DescribeMountTargetsResponse
        {
            MountTargets = [MountTarget("mt-1")],
            NextMarker = "mt-page-2"
        });
        client.MountTargetPages.Enqueue(new DescribeMountTargetsResponse { MountTargets = [MountTarget("mt-2")] });
        using var task = new EfsSourceTask();
        task.StartWith(Config(accessPoints: "false"), client);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(new string?[] { null, "mt-page-2" }, client.MountTargetMarkers);
        var mountTargets = PayloadOf(Assert.Single(records).Value)
            .GetProperty("mount_targets")
            .EnumerateArray()
            .Select(e => e.GetString() ?? string.Empty)
            .ToArray();
        Assert.Equal(
            new[] { $"mt-1:{LifeCycleState.Available}", $"mt-2:{LifeCycleState.Available}" },
            mountTargets);
    }

    [Fact]
    public async Task PollAsync_FollowsTheContinuationTokenForAccessPoints()
    {
        using var client = new ScriptedEfsClient();
        client.FileSystemPages.Enqueue(Page(FileSystem("fs-1")));
        client.AccessPointPages.Enqueue(new DescribeAccessPointsResponse
        {
            AccessPoints = [AccessPoint("ap-1")],
            NextToken = "ap-page-2"
        });
        client.AccessPointPages.Enqueue(new DescribeAccessPointsResponse { AccessPoints = [AccessPoint("ap-2")] });
        using var task = new EfsSourceTask();
        task.StartWith(Config(mountTargets: "false"), client);

        var records = await task.PollAsync(CancellationToken.None);

        Assert.Equal(new string?[] { null, "ap-page-2" }, client.AccessPointTokens);
        var accessPoints = PayloadOf(Assert.Single(records).Value)
            .GetProperty("access_points")
            .EnumerateArray()
            .Select(e => e.GetString() ?? string.Empty)
            .ToArray();
        Assert.Equal(
            new[] { $"ap-1:{LifeCycleState.Available}", $"ap-2:{LifeCycleState.Available}" },
            accessPoints);
    }

    [Fact]
    public async Task PollAsync_SkipsTheDetailLookupsThatAreTurnedOff()
    {
        using var client = new ScriptedEfsClient();
        client.FileSystemPages.Enqueue(Page(FileSystem("fs-1")));
        using var task = new EfsSourceTask();
        task.StartWith(Config(mountTargets: "false", accessPoints: "false"), client);

        Assert.Single(await task.PollAsync(CancellationToken.None));

        Assert.Empty(client.MountTargetMarkers);
        Assert.Empty(client.AccessPointTokens);
    }

    /// <summary>
    /// EFS client that answers the describe calls from scripted pages and records the
    /// pagination markers it was asked for.
    /// </summary>
    private sealed class ScriptedEfsClient : AmazonElasticFileSystemClient
    {
        public ScriptedEfsClient()
            : base(new BasicAWSCredentials("access-key", "secret-key"),
                new AmazonElasticFileSystemConfig { RegionEndpoint = RegionEndpoint.USEast1 })
        {
        }

        public Queue<DescribeFileSystemsResponse> FileSystemPages { get; } = new();

        public Queue<DescribeMountTargetsResponse> MountTargetPages { get; } = new();

        public Queue<DescribeAccessPointsResponse> AccessPointPages { get; } = new();

        public List<string?> FileSystemMarkers { get; } = [];

        public List<string?> MountTargetMarkers { get; } = [];

        public List<string?> AccessPointTokens { get; } = [];

        public override Task<DescribeFileSystemsResponse> DescribeFileSystemsAsync(
            DescribeFileSystemsRequest request, CancellationToken cancellationToken = default)
        {
            FileSystemMarkers.Add(request.Marker);
            return Task.FromResult(FileSystemPages.Count > 0
                ? FileSystemPages.Dequeue()
                : new DescribeFileSystemsResponse { FileSystems = [] });
        }

        public override Task<DescribeMountTargetsResponse> DescribeMountTargetsAsync(
            DescribeMountTargetsRequest request, CancellationToken cancellationToken = default)
        {
            MountTargetMarkers.Add(request.Marker);
            return Task.FromResult(MountTargetPages.Count > 0
                ? MountTargetPages.Dequeue()
                : new DescribeMountTargetsResponse { MountTargets = [] });
        }

        public override Task<DescribeAccessPointsResponse> DescribeAccessPointsAsync(
            DescribeAccessPointsRequest request, CancellationToken cancellationToken = default)
        {
            AccessPointTokens.Add(request.NextToken);
            return Task.FromResult(AccessPointPages.Count > 0
                ? AccessPointPages.Dequeue()
                : new DescribeAccessPointsResponse { AccessPoints = [] });
        }
    }
}
