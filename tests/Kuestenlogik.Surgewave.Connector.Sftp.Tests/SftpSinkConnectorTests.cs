using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Sftp;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sftp.Tests;

public class SftpSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new SftpSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSftpSinkTask()
    {
        var connector = new SftpSinkConnector();
        Assert.Equal(typeof(SftpSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesHostConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.HostConfig);
    }

    [Fact]
    public void Config_DefinesPortConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PortConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesPrivateKeyPathConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PrivateKeyPathConfig);
    }

    [Fact]
    public void Config_DefinesPrivateKeyPassphraseConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.PrivateKeyPassphraseConfig);
    }

    [Fact]
    public void Config_DefinesOutputPathConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.OutputPathConfig);
    }

    [Fact]
    public void Config_DefinesOutputModeConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.OutputModeConfig);
    }

    [Fact]
    public void Config_DefinesFileNameFieldConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.FileNameFieldConfig);
    }

    [Fact]
    public void Config_DefinesContentFieldConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.ContentFieldConfig);
    }

    [Fact]
    public void Config_DefinesCreateDirectoriesConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.CreateDirectoriesConfig);
    }

    [Fact]
    public void Config_DefinesOverwriteConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.OverwriteConfig);
    }

    [Fact]
    public void Config_DefinesTempSuffixConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.TempSuffixConfig);
    }

    [Fact]
    public void Config_DefinesFlushIntervalMsConfig()
    {
        var connector = new SftpSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == SftpConnectorConfig.FlushIntervalMsConfig);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new SftpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.HostConfig] = "sftp.example.com",
            [SftpConnectorConfig.UsernameConfig] = "user"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SftpConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenHostMissing()
    {
        var connector = new SftpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicsConfig] = "events",
            [SftpConnectorConfig.UsernameConfig] = "user"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SftpConnectorConfig.HostConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenUsernameMissing()
    {
        var connector = new SftpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicsConfig] = "events",
            [SftpConnectorConfig.HostConfig] = "sftp.example.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(SftpConnectorConfig.UsernameConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var connector = new SftpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicsConfig] = "events",
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
        var connector = new SftpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicsConfig] = "events",
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
        var connector = new SftpSinkConnector();
        var config = new Dictionary<string, string>
        {
            [SftpConnectorConfig.TopicsConfig] = "events",
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
    public void Config_TopicsIsHighImportance()
    {
        var connector = new SftpSinkConnector();
        var topicsKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, topicsKey.Importance);
    }

    [Fact]
    public void Config_OutputPathIsHighImportance()
    {
        var connector = new SftpSinkConnector();
        var pathKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.OutputPathConfig);
        Assert.Equal(Importance.High, pathKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedOutputModeDefault()
    {
        var connector = new SftpSinkConnector();
        var modeKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.OutputModeConfig);
        Assert.Equal(SftpConnectorConfig.DefaultOutputMode, modeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedTempSuffixDefault()
    {
        var connector = new SftpSinkConnector();
        var suffixKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.TempSuffixConfig);
        Assert.Equal(SftpConnectorConfig.DefaultTempSuffix, suffixKey.DefaultValue);
    }

    [Fact]
    public void Config_PasswordIsPasswordType()
    {
        var connector = new SftpSinkConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_PrivateKeyPassphraseIsPasswordType()
    {
        var connector = new SftpSinkConnector();
        var passphraseKey = connector.Config.Keys.First(k => k.Name == SftpConnectorConfig.PrivateKeyPassphraseConfig);
        Assert.Equal(ConfigType.Password, passphraseKey.Type);
    }
}

public class SftpSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new SftpSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PutAsync_SkipsNullValues()
    {
        using var task = new SftpSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = null! }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_SkipsEmptyValues()
    {
        using var task = new SftpSinkTask();
        var records = new List<SinkRecord>
        {
            new() { Topic = "test", Partition = 0, Offset = 0, Value = [] }
        };

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task PutAsync_HandlesEmptyRecordsList()
    {
        using var task = new SftpSinkTask();
        var records = new List<SinkRecord>();

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_CompletesSuccessfully()
    {
        using var task = new SftpSinkTask();
        var offsets = new Dictionary<TopicPartition, long>();

        var exception = await Record.ExceptionAsync(() => task.FlushAsync(offsets, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new SftpSinkTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new SftpSinkTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }
}
