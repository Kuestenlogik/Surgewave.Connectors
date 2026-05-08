using System.Text;
using System.Text.Json;
using LibGit2Sharp;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Git;

/// <summary>
/// Task that writes files to a Git repository and optionally commits changes.
/// </summary>
public sealed class GitSinkTask : SinkTask
{
    public override string Version => "1.0.0";

    private string _repositoryPath = "";
    private string _branch = GitConnectorConfig.DefaultBranch;
    private string _outputMode = GitConnectorConfig.DefaultOutputMode;
    private string _outputPath = "";
    private string _filePathField = "path";
    private string _fileContentField = "content";
    private bool _autoCommit = true;
    private bool _autoPush;
    private string _commitMessage = GitConnectorConfig.DefaultCommitMessage;
    private string _commitMessageField = "";
    private int _commitIntervalMs = GitConnectorConfig.DefaultCommitIntervalMs;
    private string _authorName = GitConnectorConfig.DefaultAuthorName;
    private string _authorEmail = GitConnectorConfig.DefaultAuthorEmail;
    private string _remote = GitConnectorConfig.DefaultRemote;
    private string _username = "";
    private string _password = "";

    private Repository? _repository;
    private DateTimeOffset _lastCommitTime = DateTimeOffset.MinValue;
    private bool _hasUncommittedChanges;

    public override void Start(IDictionary<string, string> config)
    {
        _repositoryPath = config[GitConnectorConfig.RepositoryPathConfig];

        _branch = config.TryGetValue(GitConnectorConfig.BranchConfig, out var branch)
            ? branch : GitConnectorConfig.DefaultBranch;
        _outputMode = config.TryGetValue(GitConnectorConfig.OutputModeConfig, out var mode)
            ? mode : GitConnectorConfig.DefaultOutputMode;
        _outputPath = config.TryGetValue(GitConnectorConfig.OutputPathConfig, out var path)
            ? path : "";
        _filePathField = config.TryGetValue(GitConnectorConfig.FilePathFieldConfig, out var fpf)
            ? fpf : "path";
        _fileContentField = config.TryGetValue(GitConnectorConfig.FileContentFieldConfig, out var fcf)
            ? fcf : "content";
        _autoCommit = !config.TryGetValue(GitConnectorConfig.AutoCommitConfig, out var ac) ||
                      bool.Parse(ac);
        _autoPush = config.TryGetValue(GitConnectorConfig.AutoPushConfig, out var ap) &&
                    bool.Parse(ap);
        _commitMessage = config.TryGetValue(GitConnectorConfig.CommitMessageConfig, out var cm)
            ? cm : GitConnectorConfig.DefaultCommitMessage;
        _commitMessageField = config.TryGetValue(GitConnectorConfig.CommitMessageFieldConfig, out var cmf)
            ? cmf : "";
        _commitIntervalMs = config.TryGetValue(GitConnectorConfig.CommitIntervalMsConfig, out var ci)
            ? int.Parse(ci) : GitConnectorConfig.DefaultCommitIntervalMs;
        _authorName = config.TryGetValue(GitConnectorConfig.AuthorNameConfig, out var an)
            ? an : GitConnectorConfig.DefaultAuthorName;
        _authorEmail = config.TryGetValue(GitConnectorConfig.AuthorEmailConfig, out var ae)
            ? ae : GitConnectorConfig.DefaultAuthorEmail;
        _remote = config.TryGetValue(GitConnectorConfig.RemoteConfig, out var remote)
            ? remote : GitConnectorConfig.DefaultRemote;
        _username = config.TryGetValue(GitConnectorConfig.UsernameConfig, out var user) ? user : "";
        _password = config.TryGetValue(GitConnectorConfig.PasswordConfig, out var pass) ? pass : "";

        // Open repository
        if (Directory.Exists(_repositoryPath))
        {
            _repository = new Repository(_repositoryPath);
        }
    }

    public override void Stop()
    {
        // Commit any pending changes before stopping
        if (_autoCommit && _hasUncommittedChanges && _repository != null)
        {
            try
            {
                CommitChanges(_commitMessage);
            }
            catch
            {
                // Best effort on shutdown
            }
        }

        _repository?.Dispose();
        _repository = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _repository?.Dispose();
        }
        base.Dispose(disposing);
    }

    public override Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_repository == null)
            return Task.CompletedTask;

        string? batchCommitMessage = null;

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            try
            {
                var (filePath, content, commitMsg) = ParseRecord(record);

                if (string.IsNullOrWhiteSpace(filePath))
                    continue;

                // Resolve file path
                var fullPath = Path.Combine(_repositoryPath, filePath);
                var directory = Path.GetDirectoryName(fullPath);

                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // Write file
                if (_outputMode == GitConnectorConfig.OutputModeAppend && File.Exists(fullPath))
                {
                    File.AppendAllText(fullPath, content + Environment.NewLine);
                }
                else
                {
                    File.WriteAllText(fullPath, content);
                }

                // Stage the file
                Commands.Stage(_repository, filePath);
                _hasUncommittedChanges = true;

                // Track commit message from record
                if (!string.IsNullOrWhiteSpace(commitMsg))
                {
                    batchCommitMessage = commitMsg;
                }
            }
            catch (JsonException)
            {
                // Skip invalid JSON records
            }
        }

        // Auto-commit if enabled and interval has passed
        if (_autoCommit && _hasUncommittedChanges)
        {
            var elapsed = (DateTimeOffset.UtcNow - _lastCommitTime).TotalMilliseconds;
            if (elapsed >= _commitIntervalMs)
            {
                CommitChanges(batchCommitMessage ?? _commitMessage);
            }
        }

        return Task.CompletedTask;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        // Commit any pending changes on flush
        if (_autoCommit && _hasUncommittedChanges && _repository != null)
        {
            CommitChanges(_commitMessage);
        }

        return Task.CompletedTask;
    }

    private (string filePath, string content, string? commitMessage) ParseRecord(SinkRecord record)
    {
        var json = Encoding.UTF8.GetString(record.Value);

        // Try to parse as JSON
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            var filePath = root.TryGetProperty(_filePathField, out var fp)
                ? fp.GetString() ?? ""
                : "";
            var content = root.TryGetProperty(_fileContentField, out var fc)
                ? fc.GetString() ?? json
                : json;
            var commitMsg = !string.IsNullOrWhiteSpace(_commitMessageField) &&
                           root.TryGetProperty(_commitMessageField, out var cm)
                ? cm.GetString()
                : null;

            // If no path field, use output path template
            if (string.IsNullOrWhiteSpace(filePath) && !string.IsNullOrWhiteSpace(_outputPath))
            {
                filePath = ResolveOutputPath(record);
            }

            return (filePath, content, commitMsg);
        }
        catch (JsonException)
        {
            // Not JSON - treat value as raw content
            var filePath = ResolveOutputPath(record);
            return (filePath, json, null);
        }
    }

    private string ResolveOutputPath(SinkRecord record)
    {
        if (string.IsNullOrWhiteSpace(_outputPath))
            return "";

        var path = _outputPath
            .Replace("${topic}", record.Topic)
            .Replace("${partition}", record.Partition.ToString())
            .Replace("${offset}", record.Offset.ToString())
            .Replace("${timestamp}", DateTimeOffset.UtcNow.ToString("yyyyMMddHHmmss"));

        if (record.Key != null)
        {
            var key = Encoding.UTF8.GetString(record.Key);
            // Sanitize key for filename
            key = string.Join("_", key.Split(Path.GetInvalidFileNameChars()));
            path = path.Replace("${key}", key);
        }
        else
        {
            path = path.Replace("${key}", record.Offset.ToString());
        }

        return path;
    }

    private void CommitChanges(string message)
    {
        if (_repository == null)
            return;

        try
        {
            // Check if there are staged changes
            var status = _repository.RetrieveStatus();
            if (!status.Staged.Any())
            {
                _hasUncommittedChanges = false;
                return;
            }

            var author = new Signature(_authorName, _authorEmail, DateTimeOffset.UtcNow);
            var committer = author;

            _repository.Commit(message, author, committer);
            _hasUncommittedChanges = false;
            _lastCommitTime = DateTimeOffset.UtcNow;

            // Auto-push if enabled
            if (_autoPush)
            {
                PushToRemote();
            }
        }
        catch (EmptyCommitException)
        {
            // No changes to commit
            _hasUncommittedChanges = false;
        }
    }

    private void PushToRemote()
    {
        if (_repository == null)
            return;

        try
        {
            var remote = _repository.Network.Remotes[_remote];
            if (remote == null)
                return;

            var options = new PushOptions();

            // Set up credentials if provided
            if (!string.IsNullOrWhiteSpace(_username) && !string.IsNullOrWhiteSpace(_password))
            {
                options.CredentialsProvider = (_, _, _) =>
                    new UsernamePasswordCredentials
                    {
                        Username = _username,
                        Password = _password
                    };
            }

            var refspec = $"refs/heads/{_branch}";
            _repository.Network.Push(remote, refspec, options);
        }
        catch (LibGit2SharpException)
        {
            // Push failed - will retry on next commit
        }
    }
}
