namespace Kuestenlogik.Surgewave.Connector.OneDrive.Tests;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;

public sealed class OneDriveSinkConnectorTests
{
    [Fact]
    public void OneDriveSinkConnector_HasCorrectVersion()
    {
        using var connector = new OneDriveSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void OneDriveSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new OneDriveSinkConnector();
        Assert.Equal(typeof(OneDriveSinkTask), connector.TaskClass);
    }

    [Fact]
    public void OneDriveSinkConnector_Config_HasAuthenticationKeys()
    {
        using var connector = new OneDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.TenantIdConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.ClientIdConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.ClientSecretConfig && k.Type == ConfigType.Password);
    }

    [Fact]
    public void OneDriveSinkConnector_Config_HasUserDriveKeys()
    {
        using var connector = new OneDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.UserIdConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.DriveIdConfig);
    }

    [Fact]
    public void OneDriveSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new OneDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void OneDriveSinkConnector_Config_HasUploadFolderKeys()
    {
        using var connector = new OneDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.UploadFolderPathConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.FolderIdConfig);
    }

    [Fact]
    public void OneDriveSinkConnector_Config_HasFieldMappingKeys()
    {
        using var connector = new OneDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.FileNameFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.ContentFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.MimeTypeFieldConfig);
    }

    [Fact]
    public void OneDriveSinkConnector_Config_HasUpdateModeKeys()
    {
        using var connector = new OneDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.UpdateModeConfig);
    }

    [Fact]
    public void OneDriveSinkConnector_Config_HasConflictBehaviorKeys()
    {
        using var connector = new OneDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.ConflictBehaviorConfig);
    }

    [Fact]
    public void OneDriveSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new OneDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void OneDriveSinkConnector_Config_HasRetryKeys()
    {
        using var connector = new OneDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void OneDriveSinkConnector_Start_ThrowsOnMissingTenantId()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(OneDriveConnectorConfig.TenantIdConfig, ex.Message);
    }

    [Fact]
    public void OneDriveSinkConnector_Start_ThrowsOnMissingClientId()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(OneDriveConnectorConfig.ClientIdConfig, ex.Message);
    }

    [Fact]
    public void OneDriveSinkConnector_Start_ThrowsOnMissingClientSecret()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(OneDriveConnectorConfig.ClientSecretConfig, ex.Message);
    }

    [Fact]
    public void OneDriveSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(OneDriveConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void OneDriveSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = "invalid-mode"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("invalid-mode", ex.Message);
    }

    [Fact]
    public void OneDriveSinkConnector_Start_AcceptsUploadMode()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = OneDriveConnectorConfig.ModeSinkUpload
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OneDriveSinkConnector_Start_AcceptsUpdateMode()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = OneDriveConnectorConfig.ModeSinkUpdate
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OneDriveSinkConnector_Start_ThrowsOnInvalidUpdateMode()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.UpdateModeConfig] = "invalid-update-mode"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("invalid-update-mode", ex.Message);
    }

    [Fact]
    public void OneDriveSinkConnector_Start_ThrowsOnInvalidConflictBehavior()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ConflictBehaviorConfig] = "invalid-behavior"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("invalid-behavior", ex.Message);
    }

    [Fact]
    public void OneDriveConnectorConfig_HasValidSinkModeConstants()
    {
        Assert.Equal("sink-upload", OneDriveConnectorConfig.ModeSinkUpload);
        Assert.Equal("sink-update", OneDriveConnectorConfig.ModeSinkUpdate);
    }

    [Fact]
    public void OneDriveConnectorConfig_HasValidUpdateModeConstants()
    {
        Assert.Equal("create", OneDriveConnectorConfig.UpdateModeCreate);
        Assert.Equal("replace", OneDriveConnectorConfig.UpdateModeReplace);
        Assert.Equal("create-or-replace", OneDriveConnectorConfig.UpdateModeCreateOrReplace);
    }

    [Fact]
    public void OneDriveConnectorConfig_HasValidConflictBehaviorConstants()
    {
        Assert.Equal("rename", OneDriveConnectorConfig.ConflictBehaviorRename);
        Assert.Equal("replace", OneDriveConnectorConfig.ConflictBehaviorReplace);
        Assert.Equal("fail", OneDriveConnectorConfig.ConflictBehaviorFail);
    }

    [Fact]
    public void OneDriveSinkConnector_TaskConfigs_ReturnsConfigDictionary()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Single(taskConfigs);
        Assert.Equal("test-tenant-id", taskConfigs[0][OneDriveConnectorConfig.TenantIdConfig]);
        Assert.Equal("test-client-id", taskConfigs[0][OneDriveConnectorConfig.ClientIdConfig]);
        Assert.Equal("test-client-secret", taskConfigs[0][OneDriveConnectorConfig.ClientSecretConfig]);
        Assert.Equal("test-topic", taskConfigs[0][OneDriveConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void OneDriveSinkConnector_Stop_ClearsConfig()
    {
        using var connector = new OneDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        connector.Stop();
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Single(taskConfigs);
        Assert.Empty(taskConfigs[0]);
    }

    private static ConnectorContext CreateContext()
    {
        return new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { },
            Logger = null
        };
    }
}
