namespace Kuestenlogik.Surgewave.Connector.Google.Drive.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class GoogleDriveSourceConnectorTests
{
    [Fact]
    public void GoogleDriveSourceConnector_HasCorrectVersion()
    {
        using var connector = new GoogleDriveSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void GoogleDriveSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new GoogleDriveSourceConnector();
        Assert.Equal(typeof(GoogleDriveSourceTask), connector.TaskClass);
    }

    [Fact]
    public void GoogleDriveSourceConnector_Config_HasAuthenticationKeys()
    {
        using var connector = new GoogleDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.CredentialsJsonConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.CredentialsFileConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void GoogleDriveSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new GoogleDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void GoogleDriveSourceConnector_Config_HasFolderKeys()
    {
        using var connector = new GoogleDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.FolderIdConfig);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.RecursiveConfig);
    }

    [Fact]
    public void GoogleDriveSourceConnector_Config_HasPollingKeys()
    {
        using var connector = new GoogleDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.PollIntervalMsConfig);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.TrackChangesConfig);
    }

    [Fact]
    public void GoogleDriveSourceConnector_Config_HasContentKeys()
    {
        using var connector = new GoogleDriveSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.IncludeContentConfig);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.MaxFileSizeBytesConfig);
    }

    [Fact]
    public void GoogleDriveSourceConnector_Start_ThrowsOnMissingCredentials()
    {
        using var connector = new GoogleDriveSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GoogleDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("credentials", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void GoogleDriveSourceConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new GoogleDriveSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GoogleDriveConnectorConfig.CredentialsJsonConfig] = "{\"type\":\"service_account\"}"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GoogleDriveConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void GoogleDriveSourceConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new GoogleDriveSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GoogleDriveConnectorConfig.TopicsConfig] = "test-topic",
            [GoogleDriveConnectorConfig.CredentialsJsonConfig] = "{\"type\":\"service_account\"}",
            [GoogleDriveConnectorConfig.ModeConfig] = "invalid-mode"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasValidSourceModeConstants()
    {
        Assert.Equal("source-watch", GoogleDriveConnectorConfig.ModeSourceWatch);
        Assert.Equal("source-list", GoogleDriveConnectorConfig.ModeSourceList);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectDefaults()
    {
        Assert.Equal("root", GoogleDriveConnectorConfig.DefaultFolderId);
        Assert.False(GoogleDriveConnectorConfig.DefaultRecursive);
        Assert.Equal("*", GoogleDriveConnectorConfig.DefaultFilePattern);
        Assert.Equal(30000, GoogleDriveConnectorConfig.DefaultPollIntervalMs);
        Assert.True(GoogleDriveConnectorConfig.DefaultTrackChanges);
        Assert.False(GoogleDriveConnectorConfig.DefaultIncludeContent);
        Assert.Equal(10 * 1024 * 1024, GoogleDriveConnectorConfig.DefaultMaxFileSizeBytes);
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
