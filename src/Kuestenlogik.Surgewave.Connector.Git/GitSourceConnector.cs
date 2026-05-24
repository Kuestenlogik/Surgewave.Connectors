using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Git;

/// <summary>
/// Source connector for Git repositories.
/// Watches for commits and file changes, emitting them as records.
/// </summary>
public sealed class GitSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(GitSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        .Define(GitConnectorConfig.TopicConfig, ConfigType.String, Importance.High,
            "Destination topic for git events", EditorHint.Topic)
        .Define(GitConnectorConfig.RepositoryPathConfig, ConfigType.String, Importance.High,
            "Path to the Git repository", EditorHint.FilePath)
        .Define(GitConnectorConfig.BranchConfig, ConfigType.String, GitConnectorConfig.DefaultBranch, Importance.Medium,
            "Branch to watch for changes")
        .Define(GitConnectorConfig.SourceModeConfig, ConfigType.String, GitConnectorConfig.DefaultSourceMode, Importance.Medium,
            "Source mode: 'commits' (emit commit metadata), 'files' (emit file contents), 'changes' (emit diffs)")
        .Define(GitConnectorConfig.PollIntervalMsConfig, ConfigType.Int, GitConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(GitConnectorConfig.StartFromConfig, ConfigType.String, GitConnectorConfig.DefaultStartFrom, Importance.Medium,
            "Where to start: 'latest' (new commits only) or 'earliest' (all commits)")
        .Define(GitConnectorConfig.MaxCommitsPerPollConfig, ConfigType.Int, GitConnectorConfig.DefaultMaxCommitsPerPoll, Importance.Low,
            "Maximum number of commits to process per poll")
        .Define(GitConnectorConfig.IncludeFileContentsConfig, ConfigType.Boolean, false, Importance.Low,
            "Include file contents in commit events (commits mode)")
        .Define(GitConnectorConfig.FilePatternConfig, ConfigType.String, "", Importance.Low,
            "Glob pattern to filter files (e.g., '*.cs', 'src/**')")
        .Define(GitConnectorConfig.ExcludePatternConfig, ConfigType.String, "", Importance.Low,
            "Glob pattern to exclude files (e.g., '*.log', 'bin/**')")
        .Define(GitConnectorConfig.RemoteConfig, ConfigType.String, GitConnectorConfig.DefaultRemote, Importance.Low,
            "Remote name for fetch operations")
        .Define(GitConnectorConfig.UsernameConfig, ConfigType.String, "", Importance.Low,
            "Username for remote authentication")
        .Define(GitConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.Low,
            "Password or token for remote authentication")
        .Define(GitConnectorConfig.SshKeyPathConfig, ConfigType.String, "", Importance.Low,
            "Path to SSH private key file")
        .Define(GitConnectorConfig.SshKeyPassphraseConfig, ConfigType.Password, "", Importance.Low,
            "Passphrase for SSH private key");

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(GitConnectorConfig.TopicConfig, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"Missing required config: {GitConnectorConfig.TopicConfig}");
        }

        if (!config.TryGetValue(GitConnectorConfig.RepositoryPathConfig, out var repoPath) ||
            string.IsNullOrWhiteSpace(repoPath))
        {
            throw new ArgumentException($"Missing required config: {GitConnectorConfig.RepositoryPathConfig}");
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
