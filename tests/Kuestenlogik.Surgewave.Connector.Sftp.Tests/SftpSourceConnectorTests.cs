using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Sftp;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sftp.Tests;

public class SftpSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new SftpSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSftpSourceTask()
    {
        var connector = new SftpSourceConnector();
        Assert.Equal(typeof(SftpSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesTopicConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.TopicConfig);
    }

    [Fact]
    public void Config_DefinesHostConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.HostConfig);
    }

    [Fact]
    public void Config_DefinesPortConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PortConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesPrivateKeyPathConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PrivateKeyPathConfig);
    }

    [Fact]
    public void Config_DefinesPrivateKeyPassphraseConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PrivateKeyPassphraseConfig);
    }

    [Fact]
    public void Config_DefinesPrivateKeyContentConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PrivateKeyContentConfig);
    }

    [Fact]
    public void Config_DefinesRemotePathConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.RemotePathConfig);
    }

    [Fact]
    public void Config_DefinesFilePatternConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.FilePatternConfig);
    }

    [Fact]
    public void Config_DefinesRecursiveConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.RecursiveConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalMsConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesDeleteAfterReadConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.DeleteAfterReadConfig);
    }

    [Fact]
    public void Config_DefinesMoveAfterReadConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.MoveAfterReadConfig);
    }

    [Fact]
    public void Config_DefinesMoveToPathConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.MoveToPathConfig);
    }

    [Fact]
    public void Config_DefinesIncludeMetadataConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.IncludeMetadataConfig);
    }

    [Fact]
    public void Config_DefinesMaxFileSizeBytesConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.MaxFileSizeBytesConfig);
    }

    [Fact]
    public void Config_DefinesStartFromConfig()
    {
        var connector = new SftpSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.StartFromConfig);
    }

    [Fact]
    public void Start_ThrowsWhenTopicMissing()
    {
        var connector = new SftpSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.HostConfig] = "sftp.example.com",
            [SftpConnectorConfig.UsernameConfig] = "user"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SftpConnectorConfig.TopicConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenHostMissing()
    {
        var connector = new SftpSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicConfig] = "sftp-files",
            [SftpConnectorConfig.UsernameConfig] = "user"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SftpConnectorConfig.HostConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenUsernameMissing()
    {
        var connector = new SftpSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicConfig] = "sftp-files",
            [SftpConnectorConfig.HostConfig] = "sftp.example.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SftpConnectorConfig.UsernameConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenMoveAfterReadWithoutMovePath()
    {
        var connector = new SftpSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicConfig] = "sftp-files",
            [SftpConnectorConfig.HostConfig] = "sftp.example.com",
            [SftpConnectorConfig.UsernameConfig] = "user",
            [SftpConnectorConfig.MoveAfterReadConfig] = "true"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SftpConnectorConfig.MoveToPathConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var connector = new SftpSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicConfig] = "sftp-files",
            [SftpConnectorConfig.HostConfig] = "sftp.example.com",
            [SftpConnectorConfig.UsernameConfig] = "user",
            [SftpConnectorConfig.PasswordConfig] = "password"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new SftpSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicConfig] = "sftp-files",
            [SftpConnectorConfig.HostConfig] = "sftp.example.com",
            [SftpConnectorConfig.UsernameConfig] = "user",
            [SftpConnectorConfig.PasswordConfig] = "password"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[SftpConnectorConfig.HostConfig], taskConfigs[0][SftpConnectorConfig.HostConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new SftpSourceConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicConfig] = "sftp-files",
            [SftpConnectorConfig.HostConfig] = "sftp.example.com",
            [SftpConnectorConfig.UsernameConfig] = "user",
            [SftpConnectorConfig.PasswordConfig] = "password"
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
    public void Config_PasswordIsPasswordType()
    {
        var connector = new SftpSourceConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_PrivateKeyPassphraseIsPasswordType()
    {
        var connector = new SftpSourceConnector();
        var passphraseKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.PrivateKeyPassphraseConfig);
        Assert.Equal(ConfigType.Password, passphraseKey.Type);
    }

    [Fact]
    public void Config_TopicIsHighImportance()
    {
        var connector = new SftpSourceConnector();
        var topicKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.TopicConfig);
        Assert.Equal(Importance.High, topicKey.Importance);
    }

    [Fact]
    public void Config_HostIsHighImportance()
    {
        var connector = new SftpSourceConnector();
        var hostKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.HostConfig);
        Assert.Equal(Importance.High, hostKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedPortDefault()
    {
        var connector = new SftpSourceConnector();
        var portKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.PortConfig);
        Assert.Equal(SftpConnectorConfig.DefaultPort, portKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new SftpSourceConnector();
        var pollKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.PollIntervalMsConfig);
        Assert.Equal(SftpConnectorConfig.DefaultPollIntervalMs, pollKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedStartFromDefault()
    {
        var connector = new SftpSourceConnector();
        var startKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.StartFromConfig);
        Assert.Equal(SftpConnectorConfig.DefaultStartFrom, startKey.DefaultValue);
    }
}

public class SftpSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new SftpSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new SftpSourceTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new SftpSourceTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }

    [Fact]
    public void CommitRecord_CompletesSuccessfully()
    {
        using var task = new SftpSourceTask();
        var record = new SourceRecord
        {
            Topic = "test",
            Value = [],
            SourcePartition = new Dictionary<string, object>(),
            SourceOffset = new Dictionary<string, object>()
        };
        var metadata = new RecordMetadata
        {
            Topic = "test",
            Partition = 0,
            Offset = 0
        };

        var exception = Record.Exception(() => task.CommitRecord(record, metadata));
        Assert.Null(exception);
    }

    [Fact]
    public async Task CommitAsync_CompletesSuccessfully()
    {
        using var task = new SftpSourceTask();
        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));
        Assert.Null(exception);
    }
}
