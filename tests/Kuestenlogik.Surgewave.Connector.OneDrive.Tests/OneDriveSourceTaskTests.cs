namespace Kuestenlogik.Surgewave.Connector.OneDrive.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class OneDriveSourceTaskTests
{
    [Fact]
    public void OneDriveSourceTask_HasCorrectVersion()
    {
        using var task = new OneDriveSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void OneDriveSourceTask_Start_InitializesWithValidConfig()
    {
        using var task = new OneDriveSourceTask();
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
    public void OneDriveSourceTask_Start_AcceptsAllOptions()
    {
        using var task = new OneDriveSourceTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = OneDriveConnectorConfig.ModeSourceDelta,
            [OneDriveConnectorConfig.UserIdConfig] = "user@example.com",
            [OneDriveConnectorConfig.DriveIdConfig] = "test-drive-id",
            [OneDriveConnectorConfig.FolderPathConfig] = "/documents",
            [OneDriveConnectorConfig.RecursiveConfig] = "true",
            [OneDriveConnectorConfig.FilePatternConfig] = "*.pdf",
            [OneDriveConnectorConfig.PollIntervalMsConfig] = "60000",
            [OneDriveConnectorConfig.UseDeltaQueryConfig] = "true",
            [OneDriveConnectorConfig.IncludeContentConfig] = "true",
            [OneDriveConnectorConfig.MaxFileSizeBytesConfig] = "5242880",
            [OneDriveConnectorConfig.OutputFormatConfig] = OneDriveConnectorConfig.FormatJson,
            [OneDriveConnectorConfig.BatchSizeConfig] = "20",
            [OneDriveConnectorConfig.RetryMaxConfig] = "5",
            [OneDriveConnectorConfig.RetryBackoffMsConfig] = "2000"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OneDriveSourceTask_Start_AcceptsListMode()
    {
        using var task = new OneDriveSourceTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = OneDriveConnectorConfig.ModeSourceList
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OneDriveSourceTask_Start_AcceptsBytesOutputFormat()
    {
        using var task = new OneDriveSourceTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.OutputFormatConfig] = OneDriveConnectorConfig.FormatBytes
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OneDriveSourceTask_Start_AcceptsFolderId()
    {
        using var task = new OneDriveSourceTask();
        task.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.FolderIdConfig] = "test-folder-id"
        };

        task.Start(config);
        task.Stop();
    }

    [Fact]
    public async Task OneDriveSourceTask_PollAsync_ReturnsEmptyWhenNotStarted()
    {
        using var task = new OneDriveSourceTask();
        var records = await task.PollAsync(CancellationToken.None);
        Assert.Empty(records);
    }

    [Fact]
    public async Task OneDriveSourceTask_PollAsync_ReturnsEmptyAfterStop()
    {
        using var task = new OneDriveSourceTask();
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

        var records = await task.PollAsync(CancellationToken.None);
        Assert.Empty(records);
    }

    [Fact]
    public void OneDriveSourceTask_Stop_ClearsGraphClient()
    {
        using var task = new OneDriveSourceTask();
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

        // Task should handle gracefully after stop
    }

    [Fact]
    public void OneDriveSourceTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new OneDriveSourceTask();
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
    public void OneDriveSourceTask_Initialize_DoesNotThrow()
    {
        using var task = new OneDriveSourceTask();
        var context = CreateContext();

        task.Initialize(context);
    }

    private static TaskContext CreateContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
