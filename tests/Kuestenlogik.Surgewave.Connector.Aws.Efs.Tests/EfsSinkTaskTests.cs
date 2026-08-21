using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.ElasticFileSystem;
using Amazon.ElasticFileSystem.Model;
using Amazon.Runtime;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Aws.Efs.Tests;

/// <summary>
/// Tests for <see cref="EfsSinkTask"/> driven through an EFS client whose operations are
/// recorded instead of sent to AWS.
/// </summary>
public class EfsSinkTaskTests
{
    private static Dictionary<string, string> Config(
        string? operationField = null,
        string? fileSystemIdField = null,
        string tags = "") =>
        new()
        {
            [EfsConnectorConfig.OperationFieldConfig] = operationField ?? EfsConnectorConfig.DefaultOperationField,
            [EfsConnectorConfig.FileSystemIdFieldConfig] = fileSystemIdField ?? EfsConnectorConfig.DefaultFileSystemIdField,
            [EfsConnectorConfig.PerformanceModeConfig] = "maxIO",
            [EfsConnectorConfig.ThroughputModeConfig] = "bursting",
            [EfsConnectorConfig.EncryptedConfig] = "true",
            [EfsConnectorConfig.DefaultTagsConfig] = tags
        };

    private static SinkRecord CreateRecord(string json, long offset = 0) =>
        new()
        {
            Topic = "efs-commands",
            Partition = 0,
            Offset = offset,
            Value = Encoding.UTF8.GetBytes(json)
        };

    [Fact]
    public async Task PutAsync_CreatesAFileSystemFromTheRecordAndTheConfiguredDefaults()
    {
        using var client = new RecordingEfsClient();
        using var task = new EfsSinkTask();
        task.StartWith(Config(tags: "team=platform"), client);

        await task.PutAsync(
            [CreateRecord("""{"operation":"create_file_system","name":"analytics"}""")],
            CancellationToken.None);

        var request = Assert.IsType<CreateFileSystemRequest>(Assert.Single(client.Requests));
        Assert.Equal("maxIO", request.PerformanceMode.Value);
        Assert.Equal("bursting", request.ThroughputMode.Value);
        Assert.True(request.Encrypted);
        Assert.False(string.IsNullOrEmpty(request.CreationToken));
        Assert.Contains(request.Tags, t => t.Key == "Name" && t.Value == "analytics");
        Assert.Contains(request.Tags, t => t.Key == "team" && t.Value == "platform");
        // Bursting file systems must not carry a provisioned throughput.
        Assert.Null(request.ProvisionedThroughputInMibps);
    }

    [Fact]
    public async Task PutAsync_SendsTheProvisionedThroughputOnlyForProvisionedMode()
    {
        using var client = new RecordingEfsClient();
        using var task = new EfsSinkTask();
        task.StartWith(Config(), client);

        await task.PutAsync(
            [CreateRecord("""{"operation":"create_file_system","throughput_mode":"provisioned","provisioned_throughput_mibps":123.5}""")],
            CancellationToken.None);

        var request = Assert.IsType<CreateFileSystemRequest>(Assert.Single(client.Requests));
        Assert.Equal("provisioned", request.ThroughputMode.Value);
        Assert.Equal(123.5, request.ProvisionedThroughputInMibps);
    }

    [Fact]
    public async Task PutAsync_ReadsTheOperationAndIdFromTheConfiguredFields()
    {
        using var client = new RecordingEfsClient();
        using var task = new EfsSinkTask();
        task.StartWith(Config(operationField: "action", fileSystemIdField: "fs"), client);

        await task.PutAsync(
            [CreateRecord("""{"action":"delete_file_system","fs":"fs-42"}""")],
            CancellationToken.None);

        var request = Assert.IsType<DeleteFileSystemRequest>(Assert.Single(client.Requests));
        Assert.Equal("fs-42", request.FileSystemId);
    }

    [Fact]
    public async Task PutAsync_CreatesAMountTargetWithItsSecurityGroups()
    {
        using var client = new RecordingEfsClient();
        using var task = new EfsSinkTask();
        task.StartWith(Config(), client);

        await task.PutAsync(
            [CreateRecord("""
                {"operation":"create_mount_target","file_system_id":"fs-1","subnet_id":"subnet-1",
                 "ip_address":"10.0.0.5","security_groups":["sg-1","sg-2"]}
                """)],
            CancellationToken.None);

        var request = Assert.IsType<CreateMountTargetRequest>(Assert.Single(client.Requests));
        Assert.Equal("fs-1", request.FileSystemId);
        Assert.Equal("subnet-1", request.SubnetId);
        Assert.Equal("10.0.0.5", request.IpAddress);
        Assert.Equal(new[] { "sg-1", "sg-2" }, request.SecurityGroups);
    }

    [Fact]
    public async Task PutAsync_FailsTheBatchWhenTheEfsCallFails()
    {
        // A failed call used to be swallowed per record, so the offset was committed for
        // an operation that never happened.
        var errors = new List<Exception>();
        using var client = new RecordingEfsClient { FailWith = new AmazonElasticFileSystemException("throttled") };
        using var task = new EfsSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), client);

        var thrown = await Assert.ThrowsAsync<AmazonElasticFileSystemException>(
            () => task.PutAsync([CreateRecord("""{"operation":"delete_file_system","file_system_id":"fs-1"}""")],
                CancellationToken.None));

        Assert.Same(thrown, Assert.Single(errors));
    }

    [Fact]
    public async Task PutAsync_ReportsAnUnknownOperationAndKeepsProcessingTheBatch()
    {
        // Retrying cannot fix a misspelled operation, so it is surfaced instead of ignored.
        var errors = new List<Exception>();
        using var client = new RecordingEfsClient();
        using var task = new EfsSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), client);

        await task.PutAsync(
            [
                CreateRecord("""{"operation":"delete_everything","file_system_id":"fs-1"}"""),
                CreateRecord("""{"operation":"delete_file_system","file_system_id":"fs-2"}""", 1)
            ],
            CancellationToken.None);

        var error = Assert.IsType<NotSupportedException>(Assert.Single(errors));
        Assert.Contains("delete_everything", error.Message, StringComparison.Ordinal);
        var request = Assert.IsType<DeleteFileSystemRequest>(Assert.Single(client.Requests));
        Assert.Equal("fs-2", request.FileSystemId);
    }

    [Fact]
    public async Task PutAsync_ReportsAnUnparseableRecordAndKeepsProcessingTheBatch()
    {
        var errors = new List<Exception>();
        using var client = new RecordingEfsClient();
        using var task = new EfsSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), client);

        await task.PutAsync(
            [
                CreateRecord("not json"),
                CreateRecord("""{"operation":"delete_file_system","file_system_id":"fs-2"}""", 1)
            ],
            CancellationToken.None);

        Assert.IsAssignableFrom<JsonException>(Assert.Single(errors));
        Assert.Single(client.Requests);
    }

    [Fact]
    public async Task PutAsync_IgnoresARecordWithoutAnOperation()
    {
        var errors = new List<Exception>();
        using var client = new RecordingEfsClient();
        using var task = new EfsSinkTask();
        task.Initialize(new TaskContext { RaiseError = errors.Add });
        task.StartWith(Config(), client);

        await task.PutAsync([CreateRecord("""{"file_system_id":"fs-1"}""")], CancellationToken.None);

        Assert.Empty(client.Requests);
        Assert.Empty(errors);
    }

    [Fact]
    public void GetValueHelpers_AcceptTheStringFormsOfBooleansAndNumbers()
    {
        var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
            """{"encrypted":"true","owner_uid":"1500","name":"analytics"}""")!;

        Assert.True(EfsSinkTask.GetBoolValue(data, "encrypted", false));
        Assert.Equal(1500L, EfsSinkTask.GetLongValue(data, "owner_uid", 1000));
        Assert.Equal("analytics", EfsSinkTask.GetStringValue(data, "name", ""));
        Assert.Equal("fallback", EfsSinkTask.GetStringValue(data, "missing", "fallback"));
    }

    /// <summary>
    /// EFS client that records the requests it is handed instead of calling AWS, and can be
    /// made to fail the next call.
    /// </summary>
    private sealed class RecordingEfsClient : AmazonElasticFileSystemClient
    {
        public RecordingEfsClient()
            : base(new BasicAWSCredentials("access-key", "secret-key"),
                new AmazonElasticFileSystemConfig { RegionEndpoint = RegionEndpoint.USEast1 })
        {
        }

        public List<AmazonWebServiceRequest> Requests { get; } = [];

        public Exception? FailWith { get; init; }

        public override Task<CreateFileSystemResponse> CreateFileSystemAsync(
            CreateFileSystemRequest request, CancellationToken cancellationToken = default)
            => Capture<CreateFileSystemResponse>(request);

        public override Task<DeleteFileSystemResponse> DeleteFileSystemAsync(
            DeleteFileSystemRequest request, CancellationToken cancellationToken = default)
            => Capture<DeleteFileSystemResponse>(request);

        public override Task<UpdateFileSystemResponse> UpdateFileSystemAsync(
            UpdateFileSystemRequest request, CancellationToken cancellationToken = default)
            => Capture<UpdateFileSystemResponse>(request);

        public override Task<CreateAccessPointResponse> CreateAccessPointAsync(
            CreateAccessPointRequest request, CancellationToken cancellationToken = default)
            => Capture<CreateAccessPointResponse>(request);

        public override Task<DeleteAccessPointResponse> DeleteAccessPointAsync(
            DeleteAccessPointRequest request, CancellationToken cancellationToken = default)
            => Capture<DeleteAccessPointResponse>(request);

        public override Task<CreateMountTargetResponse> CreateMountTargetAsync(
            CreateMountTargetRequest request, CancellationToken cancellationToken = default)
            => Capture<CreateMountTargetResponse>(request);

        public override Task<DeleteMountTargetResponse> DeleteMountTargetAsync(
            DeleteMountTargetRequest request, CancellationToken cancellationToken = default)
            => Capture<DeleteMountTargetResponse>(request);

        private Task<TResponse> Capture<TResponse>(AmazonWebServiceRequest request)
            where TResponse : new()
        {
            Requests.Add(request);
            return FailWith != null
                ? Task.FromException<TResponse>(FailWith)
                : Task.FromResult(new TResponse());
        }
    }
}
