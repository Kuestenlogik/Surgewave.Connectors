using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Git;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Git.Tests;

public class GitSinkConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new GitSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsGitSinkTask()
    {
        var connector = new GitSinkConnector();
        Assert.Equal(typeof(GitSinkTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesTopicsConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void Config_DefinesRepositoryPathConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.RepositoryPathConfig);
    }

    [Fact]
    public void Config_DefinesBranchConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.BranchConfig);
    }

    [Fact]
    public void Config_DefinesOutputModeConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.OutputModeConfig);
    }

    [Fact]
    public void Config_DefinesOutputPathConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.OutputPathConfig);
    }

    [Fact]
    public void Config_DefinesFilePathFieldConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.FilePathFieldConfig);
    }

    [Fact]
    public void Config_DefinesFileContentFieldConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.FileContentFieldConfig);
    }

    [Fact]
    public void Config_DefinesAutoCommitConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.AutoCommitConfig);
    }

    [Fact]
    public void Config_DefinesAutoPushConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.AutoPushConfig);
    }

    [Fact]
    public void Config_DefinesCommitMessageConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.CommitMessageConfig);
    }

    [Fact]
    public void Config_DefinesCommitMessageFieldConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.CommitMessageFieldConfig);
    }

    [Fact]
    public void Config_DefinesCommitIntervalMsConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.CommitIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesAuthorNameConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.AuthorNameConfig);
    }

    [Fact]
    public void Config_DefinesAuthorEmailConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.AuthorEmailConfig);
    }

    [Fact]
    public void Config_DefinesRemoteConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.RemoteConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesSshKeyPathConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.SshKeyPathConfig);
    }

    [Fact]
    public void Config_DefinesSshKeyPassphraseConfig()
    {
        var connector = new GitSinkConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.SshKeyPassphraseConfig);
    }

    [Fact]
    public void Start_ThrowsWhenTopicsMissing()
    {
        var connector = new GitSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.RepositoryPathConfig] = "/tmp/repo"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GitConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenRepositoryPathMissing()
    {
        var connector = new GitSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.TopicsConfig] = "events"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GitConnectorConfig.RepositoryPathConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var connector = new GitSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.TopicsConfig] = "events",
            [GitConnectorConfig.RepositoryPathConfig] = "/tmp/repo"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new GitSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.TopicsConfig] = "events",
            [GitConnectorConfig.RepositoryPathConfig] = "/tmp/repo"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(4);

        Assert.Single(taskConfigs);
        Assert.Equal(config[GitConnectorConfig.RepositoryPathConfig], taskConfigs[0][GitConnectorConfig.RepositoryPathConfig]);
    }

    [Fact]
    public void Stop_CanBeCalledMultipleTimes()
    {
        var connector = new GitSinkConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.TopicsConfig] = "events",
            [GitConnectorConfig.RepositoryPathConfig] = "/tmp/repo"
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
        var connector = new GitSinkConnector();
        var topicsKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.TopicsConfig);
        Assert.Equal(Importance.High, topicsKey.Importance);
    }

    [Fact]
    public void Config_RepositoryPathIsHighImportance()
    {
        var connector = new GitSinkConnector();
        var repoKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.RepositoryPathConfig);
        Assert.Equal(Importance.High, repoKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedBranchDefault()
    {
        var connector = new GitSinkConnector();
        var branchKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.BranchConfig);
        Assert.Equal(GitConnectorConfig.DefaultBranch, branchKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedOutputModeDefault()
    {
        var connector = new GitSinkConnector();
        var modeKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.OutputModeConfig);
        Assert.Equal(GitConnectorConfig.DefaultOutputMode, modeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedCommitMessageDefault()
    {
        var connector = new GitSinkConnector();
        var msgKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.CommitMessageConfig);
        Assert.Equal(GitConnectorConfig.DefaultCommitMessage, msgKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedCommitIntervalDefault()
    {
        var connector = new GitSinkConnector();
        var intervalKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.CommitIntervalMsConfig);
        Assert.Equal(GitConnectorConfig.DefaultCommitIntervalMs, intervalKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedAuthorNameDefault()
    {
        var connector = new GitSinkConnector();
        var nameKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.AuthorNameConfig);
        Assert.Equal(GitConnectorConfig.DefaultAuthorName, nameKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedAuthorEmailDefault()
    {
        var connector = new GitSinkConnector();
        var emailKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.AuthorEmailConfig);
        Assert.Equal(GitConnectorConfig.DefaultAuthorEmail, emailKey.DefaultValue);
    }

    [Fact]
    public void Config_PasswordIsPasswordType()
    {
        var connector = new GitSinkConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_SshKeyPassphraseIsPasswordType()
    {
        var connector = new GitSinkConnector();
        var passphraseKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.SshKeyPassphraseConfig);
        Assert.Equal(ConfigType.Password, passphraseKey.Type);
    }
}

public class GitSinkTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new GitSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PutAsync_SkipsNullValues()
    {
        using var task = new GitSinkTask();
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
        using var task = new GitSinkTask();
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
        using var task = new GitSinkTask();
        var records = new List<SinkRecord>();

        var exception = await Record.ExceptionAsync(() => task.PutAsync(records, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public async Task FlushAsync_CompletesSuccessfully()
    {
        using var task = new GitSinkTask();
        var offsets = new Dictionary<TopicPartition, long>();

        var exception = await Record.ExceptionAsync(() => task.FlushAsync(offsets, CancellationToken.None));
        Assert.Null(exception);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new GitSinkTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new GitSinkTask();

        var exception = Record.Exception(() =>
        {
            task.Dispose();
            task.Dispose();
        });

        Assert.Null(exception);
    }
}
