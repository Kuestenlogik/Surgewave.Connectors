namespace Kuestenlogik.Surgewave.Connector.Google.Drive;

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using global::Google.Apis.Auth.OAuth2;
using global::Google.Apis.Drive.v3;
using global::Google.Apis.Services;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A source task that reads files from Google Drive.
/// Supports watching for changes and listing files.
/// </summary>
public sealed class GoogleDriveSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions SerializerOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private DriveService? _driveService;
    private string _mode = GoogleDriveConnectorConfig.ModeSourceWatch;
    private string _topic = "";
    private string _folderId = GoogleDriveConnectorConfig.DefaultFolderId;
    private bool _recursive = GoogleDriveConnectorConfig.DefaultRecursive;
    private string _filePattern = GoogleDriveConnectorConfig.DefaultFilePattern;
    private string[]? _mimeTypeFilter;
    private int _pollIntervalMs = GoogleDriveConnectorConfig.DefaultPollIntervalMs;
    private bool _trackChanges = GoogleDriveConnectorConfig.DefaultTrackChanges;
    private bool _includeContent = GoogleDriveConnectorConfig.DefaultIncludeContent;
    private long _maxFileSizeBytes = GoogleDriveConnectorConfig.DefaultMaxFileSizeBytes;
    private string _outputFormat = GoogleDriveConnectorConfig.DefaultOutputFormat;
    private int _batchSize = GoogleDriveConnectorConfig.DefaultBatchSize;
    private int _retryMax = GoogleDriveConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = GoogleDriveConnectorConfig.DefaultRetryBackoffMs;

    private string? _startPageToken;
    private DateTime _lastPoll = DateTime.MinValue;
    private bool _initialListDone;
    private bool _disposed;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Initialize Google Drive service
        GoogleCredential credential;

#pragma warning disable CS0618 // GoogleCredential.FromJson/FromStream are deprecated but still functional
        if (config.TryGetValue(GoogleDriveConnectorConfig.CredentialsJsonConfig, out var credJson) && !string.IsNullOrWhiteSpace(credJson))
        {
            credential = GoogleCredential.FromJson(credJson).CreateScoped(DriveService.Scope.DriveReadonly);
        }
        else if (config.TryGetValue(GoogleDriveConnectorConfig.CredentialsFileConfig, out var credFile) && !string.IsNullOrWhiteSpace(credFile))
        {
            using var stream = new FileStream(credFile, FileMode.Open, FileAccess.Read);
            credential = GoogleCredential.FromStream(stream).CreateScoped(DriveService.Scope.DriveReadonly);
        }
#pragma warning restore CS0618
        else
        {
            throw new ArgumentException("No credentials provided");
        }

        _driveService = new DriveService(new BaseClientService.Initializer
        {
            HttpClientInitializer = credential,
            ApplicationName = "Surgewave Google Drive Connector"
        });

        if (config.TryGetValue(GoogleDriveConnectorConfig.TopicsConfig, out var topics))
            _topic = topics.Split(',')[0].Trim();

        if (config.TryGetValue(GoogleDriveConnectorConfig.ModeConfig, out var mode))
            _mode = mode;

        if (config.TryGetValue(GoogleDriveConnectorConfig.FolderIdConfig, out var folderId))
            _folderId = folderId;

        if (config.TryGetValue(GoogleDriveConnectorConfig.RecursiveConfig, out var recursive))
            _recursive = bool.Parse(recursive);

        if (config.TryGetValue(GoogleDriveConnectorConfig.FilePatternConfig, out var pattern))
            _filePattern = pattern;

        if (config.TryGetValue(GoogleDriveConnectorConfig.MimeTypeFilterConfig, out var mimeFilter) && !string.IsNullOrWhiteSpace(mimeFilter))
            _mimeTypeFilter = mimeFilter.Split(',').Select(m => m.Trim()).ToArray();

        if (config.TryGetValue(GoogleDriveConnectorConfig.PollIntervalMsConfig, out var pollInterval))
            _pollIntervalMs = int.Parse(pollInterval);

        if (config.TryGetValue(GoogleDriveConnectorConfig.TrackChangesConfig, out var trackChanges))
            _trackChanges = bool.Parse(trackChanges);

        if (config.TryGetValue(GoogleDriveConnectorConfig.IncludeContentConfig, out var includeContent))
            _includeContent = bool.Parse(includeContent);

        if (config.TryGetValue(GoogleDriveConnectorConfig.MaxFileSizeBytesConfig, out var maxSize))
            _maxFileSizeBytes = long.Parse(maxSize);

        if (config.TryGetValue(GoogleDriveConnectorConfig.OutputFormatConfig, out var outputFormat))
            _outputFormat = outputFormat;

        if (config.TryGetValue(GoogleDriveConnectorConfig.BatchSizeConfig, out var batchSize))
            _batchSize = int.Parse(batchSize);

        if (config.TryGetValue(GoogleDriveConnectorConfig.RetryMaxConfig, out var retryMax))
            _retryMax = int.Parse(retryMax);

        if (config.TryGetValue(GoogleDriveConnectorConfig.RetryBackoffMsConfig, out var retryBackoff))
            _retryBackoffMs = int.Parse(retryBackoff);
    }

    public override void Stop()
    {
        _driveService?.Dispose();
        _driveService = null;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_driveService == null) return [];

        // Check poll interval
        if (DateTime.UtcNow - _lastPoll < TimeSpan.FromMilliseconds(_pollIntervalMs))
        {
            await Task.Delay(Math.Max(100, _pollIntervalMs - (int)(DateTime.UtcNow - _lastPoll).TotalMilliseconds), cancellationToken);
        }

        _lastPoll = DateTime.UtcNow;

        try
        {
            return _mode switch
            {
                GoogleDriveConnectorConfig.ModeSourceList => await ListFilesAsync(cancellationToken),
                GoogleDriveConnectorConfig.ModeSourceWatch when _trackChanges => await WatchChangesAsync(cancellationToken),
                _ => await ListFilesAsync(cancellationToken)
            };
        }
        catch (Exception ex)
        {
            Context?.RaiseError?.Invoke(ex);
            return [];
        }
    }

    private async Task<IReadOnlyList<SourceRecord>> ListFilesAsync(CancellationToken cancellationToken)
    {
        if (_driveService == null) return [];

        var records = new List<SourceRecord>();
        var query = BuildQuery();

        var request = _driveService.Files.List();
        request.Q = query;
        request.Fields = "nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, parents, webViewLink)";
        request.PageSize = _batchSize;

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var result = await request.ExecuteAsync(cancellationToken);

                foreach (var file in result.Files ?? [])
                {
                    if (!MatchesPattern(file.Name)) continue;
                    if (_mimeTypeFilter != null && !_mimeTypeFilter.Contains(file.MimeType)) continue;

                    var record = await CreateSourceRecordAsync(file, "list", cancellationToken);
                    if (record != null)
                        records.Add(record);
                }

                _initialListDone = true;
                break;
            }
            catch (global::Google.GoogleApiException) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
            }
        }

        return records;
    }

    private async Task<IReadOnlyList<SourceRecord>> WatchChangesAsync(CancellationToken cancellationToken)
    {
        if (_driveService == null) return [];

        // Get initial start page token if not set
        if (_startPageToken == null)
        {
            var tokenRequest = _driveService.Changes.GetStartPageToken();
            var tokenResponse = await tokenRequest.ExecuteAsync(cancellationToken);
            _startPageToken = tokenResponse.StartPageTokenValue;

            // Do initial list on first run
            if (!_initialListDone)
            {
                return await ListFilesAsync(cancellationToken);
            }
        }

        var records = new List<SourceRecord>();

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var request = _driveService.Changes.List(_startPageToken);
                request.Fields = "nextPageToken, newStartPageToken, changes(fileId, file(id, name, mimeType, size, createdTime, modifiedTime, parents, webViewLink), removed)";
                request.PageSize = _batchSize;

                var result = await request.ExecuteAsync(cancellationToken);

                foreach (var change in result.Changes ?? [])
                {
                    if (change.Removed == true)
                    {
                        // File was removed
                        var removeRecord = CreateRemoveRecord(change.FileId);
                        records.Add(removeRecord);
                    }
                    else if (change.File != null)
                    {
                        if (!MatchesPattern(change.File.Name)) continue;
                        if (_mimeTypeFilter != null && !_mimeTypeFilter.Contains(change.File.MimeType)) continue;
                        if (!IsInFolder(change.File)) continue;

                        var record = await CreateSourceRecordAsync(change.File, "change", cancellationToken);
                        if (record != null)
                            records.Add(record);
                    }
                }

                _startPageToken = result.NewStartPageToken ?? result.NextPageToken;
                break;
            }
            catch (global::Google.GoogleApiException) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
            }
        }

        return records;
    }

    private string BuildQuery()
    {
        var conditions = new List<string>
        {
            "trashed = false"
        };

        if (_folderId != "root")
        {
            if (_recursive)
            {
                // Note: For deep recursion, would need to traverse folder structure
                conditions.Add($"'{_folderId}' in parents");
            }
            else
            {
                conditions.Add($"'{_folderId}' in parents");
            }
        }

        if (_mimeTypeFilter != null && _mimeTypeFilter.Length > 0)
        {
            var mimeConditions = _mimeTypeFilter.Select(m => $"mimeType = '{m}'");
            conditions.Add($"({string.Join(" or ", mimeConditions)})");
        }

        return string.Join(" and ", conditions);
    }

    private bool MatchesPattern(string? fileName)
    {
        if (string.IsNullOrEmpty(fileName)) return false;
        if (_filePattern == "*") return true;

        var regex = "^" + Regex.Escape(_filePattern).Replace("\\*", ".*").Replace("\\?", ".") + "$";
        return Regex.IsMatch(fileName, regex, RegexOptions.IgnoreCase);
    }

    private bool IsInFolder(global::Google.Apis.Drive.v3.Data.File file)
    {
        if (_folderId == "root") return true;
        return file.Parents?.Contains(_folderId) == true;
    }

    private async Task<SourceRecord?> CreateSourceRecordAsync(global::Google.Apis.Drive.v3.Data.File file, string eventType, CancellationToken cancellationToken)
    {
        if (_driveService == null) return null;

        byte[]? content = null;

        if (_includeContent && file.Size.HasValue && file.Size.Value <= _maxFileSizeBytes && !file.MimeType.StartsWith("application/vnd.google-apps.", StringComparison.Ordinal))
        {
            try
            {
                using var stream = new MemoryStream();
                var request = _driveService.Files.Get(file.Id);
                await request.DownloadAsync(stream, cancellationToken);
                content = stream.ToArray();
            }
            catch
            {
                // Ignore content download errors
            }
        }

        byte[] value;

        if (_outputFormat == GoogleDriveConnectorConfig.FormatBytes && content != null)
        {
            value = content;
        }
        else
        {
            var json = new JsonObject
            {
                ["id"] = file.Id,
                ["name"] = file.Name,
                ["mimeType"] = file.MimeType,
                ["size"] = file.Size,
                ["createdTime"] = file.CreatedTimeRaw,
                ["modifiedTime"] = file.ModifiedTimeRaw,
                ["webViewLink"] = file.WebViewLink,
                ["eventType"] = eventType
            };

            if (content != null)
            {
                json["content"] = Convert.ToBase64String(content);
                json["contentEncoding"] = "base64";
            }

            value = Encoding.UTF8.GetBytes(json.ToJsonString(SerializerOptions));
        }

        return new SourceRecord
        {
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(file.Id),
            Value = value,
            Timestamp = file.ModifiedTimeDateTimeOffset ?? DateTimeOffset.UtcNow,
            SourcePartition = new Dictionary<string, object> { ["folderId"] = _folderId },
            SourceOffset = new Dictionary<string, object> { ["fileId"] = file.Id, ["modifiedTime"] = file.ModifiedTimeRaw ?? "" }
        };
    }

    private SourceRecord CreateRemoveRecord(string fileId)
    {
        var json = new JsonObject
        {
            ["id"] = fileId,
            ["eventType"] = "removed"
        };

        return new SourceRecord
        {
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(fileId),
            Value = Encoding.UTF8.GetBytes(json.ToJsonString(SerializerOptions)),
            Timestamp = DateTimeOffset.UtcNow,
            SourcePartition = new Dictionary<string, object> { ["folderId"] = _folderId },
            SourceOffset = new Dictionary<string, object> { ["fileId"] = fileId, ["removed"] = true }
        };
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _driveService?.Dispose();
            }
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}
