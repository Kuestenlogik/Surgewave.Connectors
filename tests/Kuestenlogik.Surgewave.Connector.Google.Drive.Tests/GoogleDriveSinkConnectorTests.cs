namespace Kuestenlogik.Surgewave.Connector.Google.Drive.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class GoogleDriveSinkConnectorTests
{
    [Fact]
    public void GoogleDriveSinkConnector_HasCorrectVersion()
    {
        using var connector = new GoogleDriveSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void GoogleDriveSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new GoogleDriveSinkConnector();
        Assert.Equal(typeof(GoogleDriveSinkTask), connector.TaskClass);
    }

    [Fact]
    public void GoogleDriveSinkConnector_Config_HasAuthenticationKeys()
    {
        using var connector = new GoogleDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.CredentialsJsonConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.CredentialsFileConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void GoogleDriveSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new GoogleDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void GoogleDriveSinkConnector_Config_HasUploadKeys()
    {
        using var connector = new GoogleDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.UploadFolderIdConfig);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.FileNameFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.ContentFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.MimeTypeFieldConfig);
    }

    [Fact]
    public void GoogleDriveSinkConnector_Config_HasUpdateModeKey()
    {
        using var connector = new GoogleDriveSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GoogleDriveConnectorConfig.UpdateModeConfig);
    }

    [Fact]
    public void GoogleDriveSinkConnector_Start_ThrowsOnMissingCredentials()
    {
        using var connector = new GoogleDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GoogleDriveConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("credentials", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void GoogleDriveSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new GoogleDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GoogleDriveConnectorConfig.CredentialsJsonConfig] = "{\"type\":\"service_account\"}"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GoogleDriveConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void GoogleDriveSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new GoogleDriveSinkConnector();
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
    public void GoogleDriveSinkConnector_Start_ThrowsOnInvalidUpdateMode()
    {
        using var connector = new GoogleDriveSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GoogleDriveConnectorConfig.TopicsConfig] = "test-topic",
            [GoogleDriveConnectorConfig.CredentialsJsonConfig] = "{\"type\":\"service_account\"}",
            [GoogleDriveConnectorConfig.UpdateModeConfig] = "invalid-update-mode"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid update mode", ex.Message);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasValidSinkModeConstants()
    {
        Assert.Equal("sink-upload", GoogleDriveConnectorConfig.ModeSinkUpload);
        Assert.Equal("sink-update", GoogleDriveConnectorConfig.ModeSinkUpdate);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasValidUpdateModeConstants()
    {
        Assert.Equal("create", GoogleDriveConnectorConfig.UpdateModeCreate);
        Assert.Equal("replace", GoogleDriveConnectorConfig.UpdateModeReplace);
        Assert.Equal("create-or-replace", GoogleDriveConnectorConfig.UpdateModeCreateOrReplace);
        Assert.Equal("create-or-replace", GoogleDriveConnectorConfig.DefaultUpdateMode);
    }

    [Fact]
    public void GoogleDriveConnectorConfig_HasCorrectFieldDefaults()
    {
        Assert.Equal("filename", GoogleDriveConnectorConfig.DefaultFileNameField);
        Assert.Equal("content", GoogleDriveConnectorConfig.DefaultContentField);
        Assert.Equal("mimetype", GoogleDriveConnectorConfig.DefaultMimeTypeField);
        Assert.Equal("application/octet-stream", GoogleDriveConnectorConfig.DefaultMimeType);
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
