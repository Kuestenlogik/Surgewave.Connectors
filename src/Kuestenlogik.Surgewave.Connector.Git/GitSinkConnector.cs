using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Git;

/// <summary>
/// Sink connector for Git repositories.
/// Writes files to a repository and optionally auto-commits changes.
/// </summary>
public sealed class GitSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(GitSinkTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        .Define(GitConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        .Define(GitConnectorConfig.RepositoryPathConfig, ConfigType.String, Importance.High,
            "Path to the Git repository", EditorHint.FilePath)
        .Define(GitConnectorConfig.BranchConfig, ConfigType.String, GitConnectorConfig.DefaultBranch, Importance.Medium,
            "Branch to commit to")
        .Define(GitConnectorConfig.OutputModeConfig, ConfigType.String, GitConnectorConfig.DefaultOutputMode, Importance.Medium,
            "Output mode: 'write' (overwrite files) or 'append' (append to files)", EditorHint.Select, options: ["file", "branch"])
        .Define(GitConnectorConfig.OutputPathConfig, ConfigType.String, "", Importance.Medium,
            "Output path template (supports ${topic}, ${key}, ${timestamp})", EditorHint.FilePath)
        .Define(GitConnectorConfig.FilePathFieldConfig, ConfigType.String, "path", Importance.Medium,
            "JSON field containing the file path")
        .Define(GitConnectorConfig.FileContentFieldConfig, ConfigType.String, "content", Importance.Medium,
            "JSON field containing the file content")
        .Define(GitConnectorConfig.AutoCommitConfig, ConfigType.Boolean, true, Importance.Medium,
            "Automatically commit changes")
        .Define(GitConnectorConfig.AutoPushConfig, ConfigType.Boolean, false, Importance.Medium,
            "Automatically push commits to remote")
        .Define(GitConnectorConfig.CommitMessageConfig, ConfigType.String, GitConnectorConfig.DefaultCommitMessage, Importance.Low,
            "Default commit message")
        .Define(GitConnectorConfig.CommitMessageFieldConfig, ConfigType.String, "", Importance.Low,
            "JSON field containing the commit message (overrides default)")
        .Define(GitConnectorConfig.CommitIntervalMsConfig, ConfigType.Int, GitConnectorConfig.DefaultCommitIntervalMs, Importance.Low,
            "Interval between auto-commits in milliseconds")
        .Define(GitConnectorConfig.AuthorNameConfig, ConfigType.String, GitConnectorConfig.DefaultAuthorName, Importance.Low,
            "Author name for commits")
        .Define(GitConnectorConfig.AuthorEmailConfig, ConfigType.String, GitConnectorConfig.DefaultAuthorEmail, Importance.Low,
            "Author email for commits")
        .Define(GitConnectorConfig.RemoteConfig, ConfigType.String, GitConnectorConfig.DefaultRemote, Importance.Low,
            "Remote name for push operations")
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

        if (!config.TryGetValue(GitConnectorConfig.TopicsConfig, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"Missing required config: {GitConnectorConfig.TopicsConfig}");
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
