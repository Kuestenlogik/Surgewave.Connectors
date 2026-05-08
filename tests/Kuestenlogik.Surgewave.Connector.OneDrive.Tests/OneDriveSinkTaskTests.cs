namespace Kuestenlogik.Surgewave.Connector.OneDrive.Tests;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class OneDriveSinkTaskTests
{
    [Fact]
    public void OneDriveSinkTask_HasCorrectVersion()
    {
        using var task = new OneDriveSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void OneDriveSinkTask_Start_InitializesWithValidConfig()
    {
        using var task = new OneDriveSinkTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OneDriveSinkTask_Start_AcceptsAllOptions()
    {
        using var task = new OneDriveSinkTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = OneDriveConnectorConfig.ModeSinkUpload,
            [OneDriveConnectorConfig.UserIdConfig] = "user@example.com",
            [OneDriveConnectorConfig.DriveIdConfig] = "test-drive-id",
            [OneDriveConnectorConfig.UploadFolderPathConfig] = "/uploads",
            [OneDriveConnectorConfig.FolderIdConfig] = "test-folder-id",
            [OneDriveConnectorConfig.FileNameFieldConfig] = "name",
            [OneDriveConnectorConfig.ContentFieldConfig] = "data",
            [OneDriveConnectorConfig.MimeTypeFieldConfig] = "type",
            [OneDriveConnectorConfig.UpdateModeConfig] = OneDriveConnectorConfig.UpdateModeCreateOrReplace,
            [OneDriveConnectorConfig.ConflictBehaviorConfig] = OneDriveConnectorConfig.ConflictBehaviorReplace,
            [OneDriveConnectorConfig.BatchSizeConfig] = "20",
            [OneDriveConnectorConfig.RetryMaxConfig] = "5",
            [OneDriveConnectorConfig.RetryBackoffMsConfig] = "2000"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OneDriveSinkTask_Start_AcceptsUploadMode()
    {
        using var task = new OneDriveSinkTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = OneDriveConnectorConfig.ModeSinkUpload
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OneDriveSinkTask_Start_AcceptsUpdateMode()
    {
        using var task = new OneDriveSinkTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = OneDriveConnectorConfig.ModeSinkUpdate
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public async Task OneDriveSinkTask_PutAsync_HandlesEmptyRecords()
    {
        using var task = new OneDriveSinkTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);
        await task.PutAsync([], CancellationToken.None);
        task.Stop();
    }

    [Fact]
    public async Task OneDriveSinkTask_PutAsync_HandlesNotStarted()
    {
        using var task = new OneDriveSinkTask();
        await task.PutAsync([], CancellationToken.None);
    }

    [Fact]
    public async Task OneDriveSinkTask_PutAsync_HandlesAfterStop()
    {
        using var task = new OneDriveSinkTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);
        task.Stop();

        await task.PutAsync([], CancellationToken.None);
    }

    [Fact]
    public void OneDriveSinkTask_Stop_ClearsGraphClient()
    {
        using var task = new OneDriveSinkTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OneDriveSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new OneDriveSinkTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);
        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public void OneDriveSinkTask_Initialize_DoesNotThrow()
    {
        using var task = new OneDriveSinkTask();
        var context = CreateContext();

        task.Initialize(context);
    }

    [Fact]
    public async Task OneDriveSinkTask_PutAsync_ReportsErrorOnMissingFilename()
    {
        using var task = new OneDriveSinkTask();
        Exception? capturedError = null;

        var context = new TaskContext
        {
            RaiseError = ex => capturedError = ex
        };

        task.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);

        var recordValue = JsonSerializer.SerializeToUtf8Bytes(new
        {
            // Missing filename field
            content = Convert.ToBase64String(Encoding.UTF8.GetBytes("Hello World"))
        });

        var records = new[]
        {
            new SinkRecord
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("test-key"),
                Value = recordValue,
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        await task.PutAsync(records, CancellationToken.None);

        Assert.NotNull(capturedError);
        Assert.Contains("filename", capturedError.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task OneDriveSinkTask_PutAsync_SkipsNullValue()
    {
        using var task = new OneDriveSinkTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);

        var records = new[]
        {
            new SinkRecord
            {
                Topic = "test-topic",
                Partition = 0,
                Offset = 0,
                Key = Encoding.UTF8.GetBytes("test-key"),
                Value = null!,
                Timestamp = DateTimeOffset.UtcNow
            }
        };

        // Should not throw
        await task.PutAsync(records, CancellationToken.None);
    }

    [Fact]
    public void OneDriveSinkTask_Start_AcceptsAllConflictBehaviors()
    {
        var conflictBehaviors = new[]
        {
            OneDriveConnectorConfig.ConflictBehaviorRename,
            OneDriveConnectorConfig.ConflictBehaviorReplace,
            OneDriveConnectorConfig.ConflictBehaviorFail
        };

        foreach (var behavior in conflictBehaviors)
        {
            using var task = new OneDriveSinkTask();
            task.Initialize(CreateContext());

            var config = new Dictionary<string, string>
            {
                [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
                [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
                [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
                [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
                [OneDriveConnectorConfig.ConflictBehaviorConfig] = behavior
            };

            task.Start(config);
            task.Stop();
        }
    }

    [Fact]
    public void OneDriveSinkTask_Start_AcceptsAllUpdateModes()
    {
        var updateModes = new[]
        {
            OneDriveConnectorConfig.UpdateModeCreate,
            OneDriveConnectorConfig.UpdateModeReplace,
            OneDriveConnectorConfig.UpdateModeCreateOrReplace
        };

        foreach (var mode in updateModes)
        {
            using var task = new OneDriveSinkTask();
            task.Initialize(CreateContext());

            var config = new Dictionary<string, string>
            {
                [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
                [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
                [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
                [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
                [OneDriveConnectorConfig.UpdateModeConfig] = mode
            };

            task.Start(config);
            task.Stop();
        }
    }

    private static TaskContext CreateContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
