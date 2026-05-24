namespace Kuestenlogik.Surgewave.Connector.Git;

/// <summary>
/// Configuration constants for Git connectors.
/// </summary>
public static class GitConnectorConfig
{
    // Common
    public const string TopicConfig = "topic";
    public const string TopicsConfig = "topics";
    public const string RepositoryPathConfig = "git.repository.path";
    public const string BranchConfig = "git.branch";
    public const string RemoteConfig = "git.remote";

    // Source
    public const string SourceModeConfig = "git.source.mode";
    public const string PollIntervalMsConfig = "git.poll.interval.ms";
    public const string IncludeFileContentsConfig = "git.include.file.contents";
    public const string FilePatternConfig = "git.file.pattern";
    public const string ExcludePatternConfig = "git.exclude.pattern";
    public const string StartFromConfig = "git.start.from";
    public const string MaxCommitsPerPollConfig = "git.max.commits.per.poll";

    // Sink
    public const string OutputModeConfig = "git.output.mode";
    public const string OutputPathConfig = "git.output.path";
    public const string CommitMessageConfig = "git.commit.message";
    public const string CommitMessageFieldConfig = "git.commit.message.field";
    public const string FilePathFieldConfig = "git.file.path.field";
    public const string FileContentFieldConfig = "git.file.content.field";
    public const string AutoCommitConfig = "git.auto.commit";
    public const string AutoPushConfig = "git.auto.push";
    public const string CommitIntervalMsConfig = "git.commit.interval.ms";
    public const string AuthorNameConfig = "git.author.name";
    public const string AuthorEmailConfig = "git.author.email";

    // Authentication
    public const string UsernameConfig = "git.username";
    public const string PasswordConfig = "git.password";
    public const string SshKeyPathConfig = "git.ssh.key.path";
    public const string SshKeyPassphraseConfig = "git.ssh.key.passphrase";

    // Source modes
    public const string SourceModeCommits = "commits";
    public const string SourceModeFiles = "files";
    public const string SourceModeChanges = "changes";

    // Output modes
    public const string OutputModeWrite = "write";
    public const string OutputModeAppend = "append";

    // Start from options
    public const string StartFromLatest = "latest";
    public const string StartFromEarliest = "earliest";

    // Defaults
    public const string DefaultSourceMode = SourceModeCommits;
    public const string DefaultOutputMode = OutputModeWrite;
    public const string DefaultBranch = "main";
    public const string DefaultRemote = "origin";
    public const string DefaultStartFrom = StartFromLatest;
    public const int DefaultPollIntervalMs = 30000;
    public const int DefaultMaxCommitsPerPoll = 100;
    public const int DefaultCommitIntervalMs = 60000;
    public const string DefaultCommitMessage = "Auto-commit from Surgewave";
    public const string DefaultAuthorName = "Surgewave Connect";
    public const string DefaultAuthorEmail = "surgewave@localhost";

    // Offset keys
    public const string OffsetLastCommitSha = "last_commit_sha";
    public const string OffsetLastPoll = "last_poll";
}
