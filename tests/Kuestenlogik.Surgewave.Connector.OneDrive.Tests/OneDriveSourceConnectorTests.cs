namespace Kuestenlogik.Surgewave.Connector.OneDrive.Tests;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;

public sealed class OneDriveSourceConnectorTests
{
    [Fact]
    public void OneDriveSourceConnector_HasCorrectVersion()
    {
        using var connector = new OneDriveSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void OneDriveSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new OneDriveSourceConnector();
        Assert.Equal(typeof(OneDriveSourceTask), connector.TaskClass);
    }

    [Fact]
    public void OneDriveSourceConnector_Config_HasAuthenticationKeys()
    {
        using var connector = new OneDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.TenantIdConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.ClientIdConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.ClientSecretConfig && k.Type == ConfigType.Password);
    }

    [Fact]
    public void OneDriveSourceConnector_Config_HasUserDriveKeys()
    {
        using var connector = new OneDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.UserIdConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.DriveIdConfig);
    }

    [Fact]
    public void OneDriveSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new OneDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void OneDriveSourceConnector_Config_HasFolderKeys()
    {
        using var connector = new OneDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.FolderPathConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.FolderIdConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.RecursiveConfig);
    }

    [Fact]
    public void OneDriveSourceConnector_Config_HasPollingKeys()
    {
        using var connector = new OneDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.PollIntervalMsConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.UseDeltaQueryConfig);
    }

    [Fact]
    public void OneDriveSourceConnector_Config_HasContentKeys()
    {
        using var connector = new OneDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.IncludeContentConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.MaxFileSizeBytesConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.OutputFormatConfig);
    }

    [Fact]
    public void OneDriveSourceConnector_Config_HasBatchingKeys()
    {
        using var connector = new OneDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void OneDriveSourceConnector_Config_HasRetryKeys()
    {
        using var connector = new OneDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == OneDriveConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void OneDriveSourceConnector_Start_ThrowsOnMissingTenantId()
    {
        using var connector = new OneDriveSourceConnector();
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
    public void OneDriveSourceConnector_Start_ThrowsOnMissingClientId()
    {
        using var connector = new OneDriveSourceConnector();
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
    public void OneDriveSourceConnector_Start_ThrowsOnMissingClientSecret()
    {
        using var connector = new OneDriveSourceConnector();
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
    public void OneDriveSourceConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new OneDriveSourceConnector();
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
    public void OneDriveSourceConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new OneDriveSourceConnector();
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
    public void OneDriveSourceConnector_Start_AcceptsDeltaMode()
    {
        using var connector = new OneDriveSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = OneDriveConnectorConfig.ModeSourceDelta
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OneDriveSourceConnector_Start_AcceptsListMode()
    {
        using var connector = new OneDriveSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OneDriveConnectorConfig.TenantIdConfig] = "test-tenant-id",
            [OneDriveConnectorConfig.ClientIdConfig] = "test-client-id",
            [OneDriveConnectorConfig.ClientSecretConfig] = "test-client-secret",
            [OneDriveConnectorConfig.TopicsConfig] = "test-topic",
            [OneDriveConnectorConfig.ModeConfig] = OneDriveConnectorConfig.ModeSourceList
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OneDriveConnectorConfig_HasValidSourceModeConstants()
    {
        Assert.Equal("source-delta", OneDriveConnectorConfig.ModeSourceDelta);
        Assert.Equal("source-list", OneDriveConnectorConfig.ModeSourceList);
    }

    [Fact]
    public void OneDriveConnectorConfig_HasCorrectDefaults()
    {
        Assert.Equal("/", OneDriveConnectorConfig.DefaultFolderPath);
        Assert.False(OneDriveConnectorConfig.DefaultRecursive);
        Assert.Equal("*", OneDriveConnectorConfig.DefaultFilePattern);
        Assert.Equal(30000, OneDriveConnectorConfig.DefaultPollIntervalMs);
        Assert.True(OneDriveConnectorConfig.DefaultUseDeltaQuery);
        Assert.False(OneDriveConnectorConfig.DefaultIncludeContent);
        Assert.Equal(10 * 1024 * 1024, OneDriveConnectorConfig.DefaultMaxFileSizeBytes);
    }

    [Fact]
    public void OneDriveSourceConnector_TaskConfigs_ReturnsConfigDictionary()
    {
        using var connector = new OneDriveSourceConnector();
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
    public void OneDriveSourceConnector_Stop_ClearsConfig()
    {
        using var connector = new OneDriveSourceConnector();
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
