using Kuestenlogik.Surgewave.Connector.Git;

namespace Kuestenlogik.Surgewave.Connector.Git.Tests;

public class GitConnectorConfigTests
{
    [Fact]
    public void TopicConfig_HasExpectedValue()
    {
        Assert.Equal("topic", GitConnectorConfig.TopicConfig);
    }

    [Fact]
    public void TopicsConfig_HasExpectedValue()
    {
        Assert.Equal("topics", GitConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void RepositoryPathConfig_HasExpectedValue()
    {
        Assert.Equal("git.repository.path", GitConnectorConfig.RepositoryPathConfig);
    }

    [Fact]
    public void BranchConfig_HasExpectedValue()
    {
        Assert.Equal("git.branch", GitConnectorConfig.BranchConfig);
    }

    [Fact]
    public void RemoteConfig_HasExpectedValue()
    {
        Assert.Equal("git.remote", GitConnectorConfig.RemoteConfig);
    }

    [Fact]
    public void SourceModeConfig_HasExpectedValue()
    {
        Assert.Equal("git.source.mode", GitConnectorConfig.SourceModeConfig);
    }

    [Fact]
    public void PollIntervalMsConfig_HasExpectedValue()
    {
        Assert.Equal("git.poll.interval.ms", GitConnectorConfig.PollIntervalMsConfig);
    }

    [Fact]
    public void IncludeFileContentsConfig_HasExpectedValue()
    {
        Assert.Equal("git.include.file.contents", GitConnectorConfig.IncludeFileContentsConfig);
    }

    [Fact]
    public void FilePatternConfig_HasExpectedValue()
    {
        Assert.Equal("git.file.pattern", GitConnectorConfig.FilePatternConfig);
    }

    [Fact]
    public void ExcludePatternConfig_HasExpectedValue()
    {
        Assert.Equal("git.exclude.pattern", GitConnectorConfig.ExcludePatternConfig);
    }

    [Fact]
    public void StartFromConfig_HasExpectedValue()
    {
        Assert.Equal("git.start.from", GitConnectorConfig.StartFromConfig);
    }

    [Fact]
    public void MaxCommitsPerPollConfig_HasExpectedValue()
    {
        Assert.Equal("git.max.commits.per.poll", GitConnectorConfig.MaxCommitsPerPollConfig);
    }

    [Fact]
    public void OutputModeConfig_HasExpectedValue()
    {
        Assert.Equal("git.output.mode", GitConnectorConfig.OutputModeConfig);
    }

    [Fact]
    public void OutputPathConfig_HasExpectedValue()
    {
        Assert.Equal("git.output.path", GitConnectorConfig.OutputPathConfig);
    }

    [Fact]
    public void CommitMessageConfig_HasExpectedValue()
    {
        Assert.Equal("git.commit.message", GitConnectorConfig.CommitMessageConfig);
    }

    [Fact]
    public void CommitMessageFieldConfig_HasExpectedValue()
    {
        Assert.Equal("git.commit.message.field", GitConnectorConfig.CommitMessageFieldConfig);
    }

    [Fact]
    public void FilePathFieldConfig_HasExpectedValue()
    {
        Assert.Equal("git.file.path.field", GitConnectorConfig.FilePathFieldConfig);
    }

    [Fact]
    public void FileContentFieldConfig_HasExpectedValue()
    {
        Assert.Equal("git.file.content.field", GitConnectorConfig.FileContentFieldConfig);
    }

    [Fact]
    public void AutoCommitConfig_HasExpectedValue()
    {
        Assert.Equal("git.auto.commit", GitConnectorConfig.AutoCommitConfig);
    }

    [Fact]
    public void AutoPushConfig_HasExpectedValue()
    {
        Assert.Equal("git.auto.push", GitConnectorConfig.AutoPushConfig);
    }

    [Fact]
    public void CommitIntervalMsConfig_HasExpectedValue()
    {
        Assert.Equal("git.commit.interval.ms", GitConnectorConfig.CommitIntervalMsConfig);
    }

    [Fact]
    public void AuthorNameConfig_HasExpectedValue()
    {
        Assert.Equal("git.author.name", GitConnectorConfig.AuthorNameConfig);
    }

    [Fact]
    public void AuthorEmailConfig_HasExpectedValue()
    {
        Assert.Equal("git.author.email", GitConnectorConfig.AuthorEmailConfig);
    }

    [Fact]
    public void UsernameConfig_HasExpectedValue()
    {
        Assert.Equal("git.username", GitConnectorConfig.UsernameConfig);
    }

    [Fact]
    public void PasswordConfig_HasExpectedValue()
    {
        Assert.Equal("git.password", GitConnectorConfig.PasswordConfig);
    }

    [Fact]
    public void SshKeyPathConfig_HasExpectedValue()
    {
        Assert.Equal("git.ssh.key.path", GitConnectorConfig.SshKeyPathConfig);
    }

    [Fact]
    public void SshKeyPassphraseConfig_HasExpectedValue()
    {
        Assert.Equal("git.ssh.key.passphrase", GitConnectorConfig.SshKeyPassphraseConfig);
    }

    [Fact]
    public void SourceModeCommits_HasExpectedValue()
    {
        Assert.Equal("commits", GitConnectorConfig.SourceModeCommits);
    }

    [Fact]
    public void SourceModeFiles_HasExpectedValue()
    {
        Assert.Equal("files", GitConnectorConfig.SourceModeFiles);
    }

    [Fact]
    public void SourceModeChanges_HasExpectedValue()
    {
        Assert.Equal("changes", GitConnectorConfig.SourceModeChanges);
    }

    [Fact]
    public void OutputModeWrite_HasExpectedValue()
    {
        Assert.Equal("write", GitConnectorConfig.OutputModeWrite);
    }

    [Fact]
    public void OutputModeAppend_HasExpectedValue()
    {
        Assert.Equal("append", GitConnectorConfig.OutputModeAppend);
    }

    [Fact]
    public void StartFromLatest_HasExpectedValue()
    {
        Assert.Equal("latest", GitConnectorConfig.StartFromLatest);
    }

    [Fact]
    public void StartFromEarliest_HasExpectedValue()
    {
        Assert.Equal("earliest", GitConnectorConfig.StartFromEarliest);
    }

    [Fact]
    public void DefaultSourceMode_HasExpectedValue()
    {
        Assert.Equal("commits", GitConnectorConfig.DefaultSourceMode);
    }

    [Fact]
    public void DefaultOutputMode_HasExpectedValue()
    {
        Assert.Equal("write", GitConnectorConfig.DefaultOutputMode);
    }

    [Fact]
    public void DefaultBranch_HasExpectedValue()
    {
        Assert.Equal("main", GitConnectorConfig.DefaultBranch);
    }

    [Fact]
    public void DefaultRemote_HasExpectedValue()
    {
        Assert.Equal("origin", GitConnectorConfig.DefaultRemote);
    }

    [Fact]
    public void DefaultStartFrom_HasExpectedValue()
    {
        Assert.Equal("latest", GitConnectorConfig.DefaultStartFrom);
    }

    [Fact]
    public void DefaultPollIntervalMs_HasExpectedValue()
    {
        Assert.Equal(30000, GitConnectorConfig.DefaultPollIntervalMs);
    }

    [Fact]
    public void DefaultMaxCommitsPerPoll_HasExpectedValue()
    {
        Assert.Equal(100, GitConnectorConfig.DefaultMaxCommitsPerPoll);
    }

    [Fact]
    public void DefaultCommitIntervalMs_HasExpectedValue()
    {
        Assert.Equal(60000, GitConnectorConfig.DefaultCommitIntervalMs);
    }

    [Fact]
    public void DefaultCommitMessage_HasExpectedValue()
    {
        Assert.Equal("Auto-commit from Surgewave", GitConnectorConfig.DefaultCommitMessage);
    }

    [Fact]
    public void DefaultAuthorName_HasExpectedValue()
    {
        Assert.Equal("Surgewave Connect", GitConnectorConfig.DefaultAuthorName);
    }

    [Fact]
    public void DefaultAuthorEmail_HasExpectedValue()
    {
        Assert.Equal("surgewave@localhost", GitConnectorConfig.DefaultAuthorEmail);
    }

    [Fact]
    public void OffsetLastCommitSha_HasExpectedValue()
    {
        Assert.Equal("last_commit_sha", GitConnectorConfig.OffsetLastCommitSha);
    }

    [Fact]
    public void OffsetLastPoll_HasExpectedValue()
    {
        Assert.Equal("last_poll", GitConnectorConfig.OffsetLastPoll);
    }
}
