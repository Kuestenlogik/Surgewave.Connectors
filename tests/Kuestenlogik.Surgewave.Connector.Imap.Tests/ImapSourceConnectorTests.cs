using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Imap;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Imap.Tests;

public class ImapSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new ImapSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsImapSourceTask()
    {
        var connector = new ImapSourceConnector();
        Assert.Equal(typeof(ImapSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesTopicConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.TopicConfig);
    }

    [Fact]
    public void Config_DefinesHostConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.HostConfig);
    }

    [Fact]
    public void Config_DefinesPortConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.PortConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesUseSslConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.UseSslConfig);
    }

    [Fact]
    public void Config_DefinesTimeoutSecondsConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.TimeoutSecondsConfig);
    }

    [Fact]
    public void Config_DefinesAcceptInvalidCertificatesConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.AcceptInvalidCertificatesConfig);
    }

    [Fact]
    public void Config_DefinesFolderConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.FolderConfig);
    }

    [Fact]
    public void Config_DefinesFoldersConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.FoldersConfig);
    }

    [Fact]
    public void Config_DefinesRecursiveConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.RecursiveConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalMsConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesUseIdleConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.UseIdleConfig);
    }

    [Fact]
    public void Config_DefinesIdleTimeoutMinutesConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.IdleTimeoutMinutesConfig);
    }

    [Fact]
    public void Config_DefinesBatchSizeConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.BatchSizeConfig);
    }

    [Fact]
    public void Config_DefinesMarkAsReadConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.MarkAsReadConfig);
    }

    [Fact]
    public void Config_DefinesDeleteAfterReadConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.DeleteAfterReadConfig);
    }

    [Fact]
    public void Config_DefinesMoveAfterReadConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.MoveAfterReadConfig);
    }

    [Fact]
    public void Config_DefinesMoveToFolderConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.MoveToFolderConfig);
    }

    [Fact]
    public void Config_DefinesStartFromConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.StartFromConfig);
    }

    [Fact]
    public void Config_DefinesUnseenOnlyConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.UnseenOnlyConfig);
    }

    [Fact]
    public void Config_DefinesSinceConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.SinceConfig);
    }

    [Fact]
    public void Config_DefinesSubjectFilterConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.SubjectFilterConfig);
    }

    [Fact]
    public void Config_DefinesFromFilterConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.FromFilterConfig);
    }

    [Fact]
    public void Config_DefinesIncludeBodyConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.IncludeBodyConfig);
    }

    [Fact]
    public void Config_DefinesIncludeAttachmentsConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.IncludeAttachmentsConfig);
    }

    [Fact]
    public void Config_DefinesMaxAttachmentSizeBytesConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.MaxAttachmentSizeBytesConfig);
    }

    [Fact]
    public void Config_DefinesPreferHtmlConfig()
    {
        var connector = new ImapSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == ImapConnectorConfig.PreferHtmlConfig);
    }

    [Fact]
    public void Start_ThrowsWhenTopicMissing()
    {
        var connector = new ImapSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ImapConnectorConfig.TopicConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenHostMissing()
    {
        var connector = new ImapSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ImapConnectorConfig.HostConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenUsernameMissing()
    {
        var connector = new ImapSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ImapConnectorConfig.UsernameConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenMoveAfterReadWithoutMoveToFolder()
    {
        var connector = new ImapSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.MoveAfterReadConfig] = "true"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ImapConnectorConfig.MoveToFolderConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var connector = new ImapSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void Start_SucceedsWithMoveAfterReadAndMoveToFolder()
    {
        var connector = new ImapSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com",
            [ImapConnectorConfig.MoveAfterReadConfig] = "true",
            [ImapConnectorConfig.MoveToFolderConfig] = "Archive"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new ImapSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[ImapConnectorConfig.HostConfig], taskConfigs[0][ImapConnectorConfig.HostConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new ImapSourceConnector();
        var config = new Dictionary<string, string>
        {
            [ImapConnectorConfig.TopicConfig] = "emails",
            [ImapConnectorConfig.HostConfig] = "imap.example.com",
            [ImapConnectorConfig.UsernameConfig] = "user@example.com"
        };

        connector.Start(config);

        var exception = Record.Exception(() =>
        {
            connector.Stop();
            connector.Stop();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void Config_TopicIsHighImportance()
    {
        var connector = new ImapSourceConnector();
        var topicKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.TopicConfig);
        Assert.Equal(Importance.High, topicKey.Importance);
    }

    [Fact]
    public void Config_HostIsHighImportance()
    {
        var connector = new ImapSourceConnector();
        var hostKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.HostConfig);
        Assert.Equal(Importance.High, hostKey.Importance);
    }

    [Fact]
    public void Config_UsernameIsHighImportance()
    {
        var connector = new ImapSourceConnector();
        var usernameKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.UsernameConfig);
        Assert.Equal(Importance.High, usernameKey.Importance);
    }

    [Fact]
    public void Config_PasswordIsPasswordType()
    {
        var connector = new ImapSourceConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_HasExpectedPortDefault()
    {
        var connector = new ImapSourceConnector();
        var portKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.PortConfig);
        Assert.Equal(ImapConnectorConfig.DefaultPort, portKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedTimeoutDefault()
    {
        var connector = new ImapSourceConnector();
        var timeoutKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.TimeoutSecondsConfig);
        Assert.Equal(ImapConnectorConfig.DefaultTimeoutSeconds, timeoutKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedFolderDefault()
    {
        var connector = new ImapSourceConnector();
        var folderKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.FolderConfig);
        Assert.Equal(ImapConnectorConfig.DefaultFolder, folderKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new ImapSourceConnector();
        var pollKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.PollIntervalMsConfig);
        Assert.Equal(ImapConnectorConfig.DefaultPollIntervalMs, pollKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedBatchSizeDefault()
    {
        var connector = new ImapSourceConnector();
        var batchKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.BatchSizeConfig);
        Assert.Equal(ImapConnectorConfig.DefaultBatchSize, batchKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedStartFromDefault()
    {
        var connector = new ImapSourceConnector();
        var startFromKey = connector.Config.Keys.First(k => k.Name == ImapConnectorConfig.StartFromConfig);
        Assert.Equal(ImapConnectorConfig.DefaultStartFrom, startFromKey.DefaultValue);
    }
}
