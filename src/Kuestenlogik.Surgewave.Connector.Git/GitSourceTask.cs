using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using LibGit2Sharp;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Git;

internal static class JsonOptions
{
    public static readonly JsonSerializerOptions Default = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };
}

/// <summary>
/// Task that watches a Git repository for changes and produces commit/file events as records.
/// </summary>
public sealed class GitSourceTask : SourceTask
{
    public override string Version => "1.0.0";

    private string _topic = "";
    private string _repositoryPath = "";
    private string _branch = GitConnectorConfig.DefaultBranch;
    private string _sourceMode = GitConnectorConfig.DefaultSourceMode;
    private string _startFrom = GitConnectorConfig.DefaultStartFrom;
    private int _pollIntervalMs = GitConnectorConfig.DefaultPollIntervalMs;
    private int _maxCommitsPerPoll = GitConnectorConfig.DefaultMaxCommitsPerPoll;
    private bool _includeFileContents;
    private Regex? _filePattern;
    private Regex? _excludePattern;

    private Repository? _repository;
    private string? _lastCommitSha;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;
    private readonly Dictionary<string, object> _sourcePartition = [];
    private bool _initialized;

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[GitConnectorConfig.TopicConfig];
        _repositoryPath = config[GitConnectorConfig.RepositoryPathConfig];

        _branch = config.TryGetValue(GitConnectorConfig.BranchConfig, out var branch)
            ? branch : GitConnectorConfig.DefaultBranch;
        _sourceMode = config.TryGetValue(GitConnectorConfig.SourceModeConfig, out var mode)
            ? mode : GitConnectorConfig.DefaultSourceMode;
        _startFrom = config.TryGetValue(GitConnectorConfig.StartFromConfig, out var start)
            ? start : GitConnectorConfig.DefaultStartFrom;
        _pollIntervalMs = config.TryGetValue(GitConnectorConfig.PollIntervalMsConfig, out var poll)
            ? int.Parse(poll) : GitConnectorConfig.DefaultPollIntervalMs;
        _maxCommitsPerPoll = config.TryGetValue(GitConnectorConfig.MaxCommitsPerPollConfig, out var max)
            ? int.Parse(max) : GitConnectorConfig.DefaultMaxCommitsPerPoll;
        _includeFileContents = config.TryGetValue(GitConnectorConfig.IncludeFileContentsConfig, out var inc)
            && bool.Parse(inc);

        // Compile file patterns
        if (config.TryGetValue(GitConnectorConfig.FilePatternConfig, out var pattern) &&
            !string.IsNullOrWhiteSpace(pattern))
        {
            _filePattern = GlobToRegex(pattern);
        }

        if (config.TryGetValue(GitConnectorConfig.ExcludePatternConfig, out var exclude) &&
            !string.IsNullOrWhiteSpace(exclude))
        {
            _excludePattern = GlobToRegex(exclude);
        }

        _sourcePartition["repository"] = _repositoryPath;
        _sourcePartition["branch"] = _branch;

        // Restore offset
        var storedOffset = Context?.OffsetStorageReader?.Offset(_sourcePartition);
        if (storedOffset != null &&
            storedOffset.TryGetValue(GitConnectorConfig.OffsetLastCommitSha, out var lastSha))
        {
            _lastCommitSha = lastSha?.ToString();
        }
    }

    public override void Stop()
    {
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

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastPollTime).TotalMilliseconds;

        if (elapsed < _pollIntervalMs)
        {
            var waitTime = (int)(_pollIntervalMs - elapsed);
            await Task.Delay(waitTime, cancellationToken);
        }

        try
        {
            // Initialize repository on first poll
            if (_repository == null)
            {
                if (!Directory.Exists(_repositoryPath))
                    return [];

                _repository = new Repository(_repositoryPath);
            }

            // Get the branch
            var branchRef = _repository.Branches[_branch] ??
                           _repository.Branches[$"origin/{_branch}"] ??
                           _repository.Branches[$"refs/heads/{_branch}"];

            if (branchRef == null)
            {
                _lastPollTime = DateTimeOffset.UtcNow;
                return [];
            }

            // Determine starting point
            if (!_initialized)
            {
                _initialized = true;
                if (_lastCommitSha == null && _startFrom == GitConnectorConfig.StartFromLatest)
                {
                    // Start from current HEAD, only process new commits
                    _lastCommitSha = branchRef.Tip.Sha;
                    _lastPollTime = DateTimeOffset.UtcNow;
                    return [];
                }
            }

            // Get commits since last known commit
            var commits = GetNewCommits(branchRef, _lastCommitSha, _maxCommitsPerPoll);

            if (commits.Count == 0)
            {
                _lastPollTime = DateTimeOffset.UtcNow;
                return [];
            }

            var records = new List<SourceRecord>();

            foreach (var commit in commits)
            {
                var commitRecords = _sourceMode switch
                {
                    GitConnectorConfig.SourceModeCommits => CreateCommitRecords(commit),
                    GitConnectorConfig.SourceModeFiles => CreateFileRecords(commit),
                    GitConnectorConfig.SourceModeChanges => CreateChangeRecords(commit),
                    _ => CreateCommitRecords(commit)
                };

                records.AddRange(commitRecords);
                _lastCommitSha = commit.Sha;
            }

            _lastPollTime = DateTimeOffset.UtcNow;
            return records;
        }
        catch (RepositoryNotFoundException)
        {
            await Task.Delay(5000, cancellationToken);
            return [];
        }
        catch (LibGit2SharpException)
        {
            await Task.Delay(5000, cancellationToken);
            return [];
        }
    }

    private List<Commit> GetNewCommits(Branch branch, string? sinceCommitSha, int maxCount)
    {
        var commits = new List<Commit>();
        var filter = new CommitFilter
        {
            IncludeReachableFrom = branch.Tip,
            SortBy = CommitSortStrategies.Topological | CommitSortStrategies.Reverse
        };

        if (!string.IsNullOrEmpty(sinceCommitSha))
        {
            var sinceCommit = _repository?.Lookup<Commit>(sinceCommitSha);
            if (sinceCommit != null)
            {
                filter.ExcludeReachableFrom = sinceCommit;
            }
        }

        foreach (var commit in _repository!.Commits.QueryBy(filter))
        {
            if (commits.Count >= maxCount)
                break;

            commits.Add(commit);
        }

        return commits;
    }

    private List<SourceRecord> CreateCommitRecords(Commit commit)
    {
        var records = new List<SourceRecord>();

        var commitData = new Dictionary<string, object?>
        {
            ["sha"] = commit.Sha,
            ["shortSha"] = commit.Sha[..7],
            ["message"] = commit.Message,
            ["messageShort"] = commit.MessageShort,
            ["author"] = new Dictionary<string, object?>
            {
                ["name"] = commit.Author.Name,
                ["email"] = commit.Author.Email,
                ["when"] = commit.Author.When.ToString("o")
            },
            ["committer"] = new Dictionary<string, object?>
            {
                ["name"] = commit.Committer.Name,
                ["email"] = commit.Committer.Email,
                ["when"] = commit.Committer.When.ToString("o")
            },
            ["parents"] = commit.Parents.Select(p => p.Sha).ToList(),
            ["branch"] = _branch
        };

        // Include file changes summary
        var changes = GetCommitChanges(commit);
        commitData["filesChanged"] = changes.Count;
        commitData["files"] = changes.Select(c => new Dictionary<string, object?>
        {
            ["path"] = c.Path,
            ["status"] = c.Status.ToString(),
            ["oldPath"] = c.OldPath
        }).ToList();

        // Optionally include file contents
        if (_includeFileContents)
        {
            commitData["fileContents"] = GetFileContents(commit, changes);
        }

        var json = JsonSerializer.Serialize(commitData, JsonOptions.Default);

        records.Add(new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                [GitConnectorConfig.OffsetLastCommitSha] = commit.Sha,
                [GitConnectorConfig.OffsetLastPoll] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(commit.Sha),
            Value = Encoding.UTF8.GetBytes(json),
            Timestamp = commit.Author.When
        });

        return records;
    }

    private List<SourceRecord> CreateFileRecords(Commit commit)
    {
        var records = new List<SourceRecord>();
        var changes = GetCommitChanges(commit);

        foreach (var change in changes)
        {
            if (!MatchesFilePattern(change.Path))
                continue;

            string? content = null;
            if (change.Status != ChangeKind.Deleted)
            {
                var blob = commit[change.Path]?.Target as Blob;
                if (blob != null && !blob.IsBinary)
                {
                    content = blob.GetContentText();
                }
            }

            var fileData = new Dictionary<string, object?>
            {
                ["path"] = change.Path,
                ["oldPath"] = change.OldPath,
                ["status"] = change.Status.ToString(),
                ["commit"] = commit.Sha,
                ["commitMessage"] = commit.MessageShort,
                ["author"] = commit.Author.Name,
                ["authorEmail"] = commit.Author.Email,
                ["timestamp"] = commit.Author.When.ToString("o"),
                ["content"] = content,
                ["isBinary"] = change.Status != ChangeKind.Deleted &&
                              ((commit[change.Path]?.Target as Blob)?.IsBinary ?? false)
            };

            var json = JsonSerializer.Serialize(fileData, JsonOptions.Default);

            records.Add(new SourceRecord
            {
                SourcePartition = _sourcePartition,
                SourceOffset = new Dictionary<string, object>
                {
                    [GitConnectorConfig.OffsetLastCommitSha] = commit.Sha,
                    [GitConnectorConfig.OffsetLastPoll] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                },
                Topic = _topic,
                Key = Encoding.UTF8.GetBytes($"{commit.Sha}:{change.Path}"),
                Value = Encoding.UTF8.GetBytes(json),
                Timestamp = commit.Author.When
            });
        }

        return records;
    }

    private List<SourceRecord> CreateChangeRecords(Commit commit)
    {
        var records = new List<SourceRecord>();
        var parent = commit.Parents.FirstOrDefault();

        if (parent == null && _repository != null)
        {
            // First commit - compare against empty tree
            foreach (var entry in commit.Tree)
            {
                if (!MatchesFilePattern(entry.Path))
                    continue;

                var changeData = new Dictionary<string, object?>
                {
                    ["path"] = entry.Path,
                    ["status"] = "Added",
                    ["commit"] = commit.Sha,
                    ["commitMessage"] = commit.MessageShort,
                    ["author"] = commit.Author.Name,
                    ["timestamp"] = commit.Author.When.ToString("o"),
                    ["linesAdded"] = 0,
                    ["linesDeleted"] = 0
                };

                if (entry.Target is Blob blob && !blob.IsBinary)
                {
                    var content = blob.GetContentText();
                    changeData["linesAdded"] = content.Split('\n').Length;
                    changeData["newContent"] = content;
                }

                var json = JsonSerializer.Serialize(changeData, JsonOptions.Default);

                records.Add(new SourceRecord
                {
                    SourcePartition = _sourcePartition,
                    SourceOffset = new Dictionary<string, object>
                    {
                        [GitConnectorConfig.OffsetLastCommitSha] = commit.Sha,
                        [GitConnectorConfig.OffsetLastPoll] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    },
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes($"{commit.Sha}:{entry.Path}"),
                    Value = Encoding.UTF8.GetBytes(json),
                    Timestamp = commit.Author.When
                });
            }
        }
        else if (parent != null && _repository != null)
        {
            var patch = _repository.Diff.Compare<Patch>(parent.Tree, commit.Tree);

            foreach (var entry in patch)
            {
                if (!MatchesFilePattern(entry.Path))
                    continue;

                var changeData = new Dictionary<string, object?>
                {
                    ["path"] = entry.Path,
                    ["oldPath"] = entry.OldPath,
                    ["status"] = entry.Status.ToString(),
                    ["commit"] = commit.Sha,
                    ["commitMessage"] = commit.MessageShort,
                    ["author"] = commit.Author.Name,
                    ["timestamp"] = commit.Author.When.ToString("o"),
                    ["linesAdded"] = entry.LinesAdded,
                    ["linesDeleted"] = entry.LinesDeleted,
                    ["patch"] = entry.Patch,
                    ["isBinary"] = entry.IsBinaryComparison
                };

                var json = JsonSerializer.Serialize(changeData, JsonOptions.Default);

                records.Add(new SourceRecord
                {
                    SourcePartition = _sourcePartition,
                    SourceOffset = new Dictionary<string, object>
                    {
                        [GitConnectorConfig.OffsetLastCommitSha] = commit.Sha,
                        [GitConnectorConfig.OffsetLastPoll] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                    },
                    Topic = _topic,
                    Key = Encoding.UTF8.GetBytes($"{commit.Sha}:{entry.Path}"),
                    Value = Encoding.UTF8.GetBytes(json),
                    Timestamp = commit.Author.When
                });
            }
        }

        return records;
    }

    private List<TreeEntryChanges> GetCommitChanges(Commit commit)
    {
        var parent = commit.Parents.FirstOrDefault();
        if (parent == null || _repository == null)
        {
            // First commit - all files are added
            return commit.Tree
                .Where(e => MatchesFilePattern(e.Path))
                .Select(e => new TreeEntryChanges(e.Path, ChangeKind.Added))
                .ToList();
        }

        var changes = _repository.Diff.Compare<TreeChanges>(parent.Tree, commit.Tree);
        return changes
            .Where(c => MatchesFilePattern(c.Path))
            .Select(c => new TreeEntryChanges(c.Path, c.Status, c.OldPath))
            .ToList();
    }

    private Dictionary<string, string?> GetFileContents(Commit commit, List<TreeEntryChanges> changes)
    {
        var contents = new Dictionary<string, string?>();

        foreach (var change in changes)
        {
            if (change.Status == ChangeKind.Deleted)
            {
                contents[change.Path] = null;
                continue;
            }

            var blob = commit[change.Path]?.Target as Blob;
            if (blob != null && !blob.IsBinary)
            {
                contents[change.Path] = blob.GetContentText();
            }
        }

        return contents;
    }

    private bool MatchesFilePattern(string path)
    {
        if (_excludePattern != null && _excludePattern.IsMatch(path))
            return false;

        if (_filePattern != null)
            return _filePattern.IsMatch(path);

        return true;
    }

    private static Regex GlobToRegex(string glob)
    {
        var pattern = "^" + Regex.Escape(glob)
            .Replace("\\*\\*", ".*")
            .Replace("\\*", "[^/]*")
            .Replace("\\?", ".") + "$";

        return new Regex(pattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);
    }

    private sealed record TreeEntryChanges(string Path, ChangeKind Status, string? OldPath = null);
}
