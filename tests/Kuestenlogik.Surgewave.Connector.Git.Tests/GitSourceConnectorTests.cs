using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connector.Git;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Git.Tests;

public class GitSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        var connector = new GitSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsGitSourceTask()
    {
        var connector = new GitSourceConnector();
        Assert.Equal(typeof(GitSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_DefinesTopicConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.TopicConfig);
    }

    [Fact]
    public void Config_DefinesRepositoryPathConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.RepositoryPathConfig);
    }

    [Fact]
    public void Config_DefinesBranchConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.BranchConfig);
    }

    [Fact]
    public void Config_DefinesSourceModeConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.SourceModeConfig);
    }

    [Fact]
    public void Config_DefinesPollIntervalMsConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void Config_DefinesStartFromConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.StartFromConfig);
    }

    [Fact]
    public void Config_DefinesMaxCommitsPerPollConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.MaxCommitsPerPollConfig);
    }

    [Fact]
    public void Config_DefinesIncludeFileContentsConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.IncludeFileContentsConfig);
    }

    [Fact]
    public void Config_DefinesFilePatternConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.FilePatternConfig);
    }

    [Fact]
    public void Config_DefinesExcludePatternConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.ExcludePatternConfig);
    }

    [Fact]
    public void Config_DefinesRemoteConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.RemoteConfig);
    }

    [Fact]
    public void Config_DefinesUsernameConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void Config_DefinesPasswordConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void Config_DefinesSshKeyPathConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.SshKeyPathConfig);
    }

    [Fact]
    public void Config_DefinesSshKeyPassphraseConfig()
    {
        var connector = new GitSourceConnector();
        Assert.Contains(connector.Config.Keys, k => k.Name == GitConnectorConfig.SshKeyPassphraseConfig);
    }

    [Fact]
    public void Start_ThrowsWhenTopicMissing()
    {
        var connector = new GitSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.RepositoryPathConfig] = "/tmp/repo"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GitConnectorConfig.TopicConfig, ex.Message);
    }

    [Fact]
    public void Start_ThrowsWhenRepositoryPathMissing()
    {
        var connector = new GitSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.TopicConfig] = "git-events"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GitConnectorConfig.RepositoryPathConfig, ex.Message);
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var connector = new GitSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.TopicConfig] = "git-events",
            [GitConnectorConfig.RepositoryPathConfig] = "/tmp/repo"
        };

        var exception = Record.Exception(() => connector.Start(config));
        Assert.Null(exception);
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var connector = new GitSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.TopicConfig] = "git-events",
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
        var connector = new GitSourceConnector();
        var config = new Dictionary<string, string>
        {
            [GitConnectorConfig.TopicConfig] = "git-events",
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
    public void Config_PasswordIsPasswordType()
    {
        var connector = new GitSourceConnector();
        var passwordKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.PasswordConfig);
        Assert.Equal(ConfigType.Password, passwordKey.Type);
    }

    [Fact]
    public void Config_SshKeyPassphraseIsPasswordType()
    {
        var connector = new GitSourceConnector();
        var passphraseKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.SshKeyPassphraseConfig);
        Assert.Equal(ConfigType.Password, passphraseKey.Type);
    }

    [Fact]
    public void Config_TopicIsHighImportance()
    {
        var connector = new GitSourceConnector();
        var topicKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.TopicConfig);
        Assert.Equal(Importance.High, topicKey.Importance);
    }

    [Fact]
    public void Config_RepositoryPathIsHighImportance()
    {
        var connector = new GitSourceConnector();
        var repoKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.RepositoryPathConfig);
        Assert.Equal(Importance.High, repoKey.Importance);
    }

    [Fact]
    public void Config_HasExpectedBranchDefault()
    {
        var connector = new GitSourceConnector();
        var branchKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.BranchConfig);
        Assert.Equal(GitConnectorConfig.DefaultBranch, branchKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedSourceModeDefault()
    {
        var connector = new GitSourceConnector();
        var modeKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.SourceModeConfig);
        Assert.Equal(GitConnectorConfig.DefaultSourceMode, modeKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedPollIntervalDefault()
    {
        var connector = new GitSourceConnector();
        var pollKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.PollIntervalMsConfig);
        Assert.Equal(GitConnectorConfig.DefaultPollIntervalMs, pollKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedStartFromDefault()
    {
        var connector = new GitSourceConnector();
        var startKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.StartFromConfig);
        Assert.Equal(GitConnectorConfig.DefaultStartFrom, startKey.DefaultValue);
    }

    [Fact]
    public void Config_HasExpectedMaxCommitsDefault()
    {
        var connector = new GitSourceConnector();
        var maxKey = connector.Config.Keys.First(k => k.Name == GitConnectorConfig.MaxCommitsPerPollConfig);
        Assert.Equal(GitConnectorConfig.DefaultMaxCommitsPerPoll, maxKey.DefaultValue);
    }
}

public class GitSourceTaskTests
{
    [Fact]
    public void Version_ReturnsExpectedValue()
    {
        using var task = new GitSourceTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public async Task PollAsync_ReturnsEmptyWhenRepositoryNotExists()
    {
        using var task = new GitSourceTask();
        task.Start(new Dictionary<string, string>
        {
            [GitConnectorConfig.TopicConfig] = "test",
            [GitConnectorConfig.RepositoryPathConfig] = "/non/existent/path",
            [GitConnectorConfig.PollIntervalMsConfig] = "0"
        });

        var result = await task.PollAsync(CancellationToken.None);
        Assert.Empty(result);
    }

    [Fact]
    public void Stop_CanBeCalledWithoutStart()
    {
        using var task = new GitSourceTask();
        var exception = Record.Exception(() => task.Stop());
        Assert.Null(exception);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var task = new GitSourceTask();

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
        using var task = new GitSourceTask();
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
        using var task = new GitSourceTask();
        var exception = await Record.ExceptionAsync(() => task.CommitAsync(CancellationToken.None));
        Assert.Null(exception);
    }
}
