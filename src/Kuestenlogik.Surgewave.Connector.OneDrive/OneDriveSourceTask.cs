namespace Kuestenlogik.Surgewave.Connector.OneDrive;

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using global::Azure.Identity;
using global::Microsoft.Graph;
using global::Microsoft.Graph.Drives.Item.Items.Item.Delta;
using global::Microsoft.Graph.Models;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A source task that reads files from OneDrive via Microsoft Graph API.
/// Supports delta queries for efficient change tracking.
/// </summary>
public sealed class OneDriveSourceTask : SourceTask
{
    private static readonly JsonSerializerOptions SerializerOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private GraphServiceClient? _graphClient;
    private string _mode = OneDriveConnectorConfig.ModeSourceDelta;
    private string _topic = "";
    private string? _userId;
    private string? _driveId;
    private string _folderPath = OneDriveConnectorConfig.DefaultFolderPath;
    private string? _folderId;
    private bool _recursive = OneDriveConnectorConfig.DefaultRecursive;
    private string _filePattern = OneDriveConnectorConfig.DefaultFilePattern;
    private int _pollIntervalMs = OneDriveConnectorConfig.DefaultPollIntervalMs;
    private bool _useDeltaQuery = OneDriveConnectorConfig.DefaultUseDeltaQuery;
    private bool _includeContent = OneDriveConnectorConfig.DefaultIncludeContent;
    private long _maxFileSizeBytes = OneDriveConnectorConfig.DefaultMaxFileSizeBytes;
    private string _outputFormat = OneDriveConnectorConfig.DefaultOutputFormat;
    private int _batchSize = OneDriveConnectorConfig.DefaultBatchSize;
    private int _retryMax = OneDriveConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = OneDriveConnectorConfig.DefaultRetryBackoffMs;

    private string? _deltaLink;
    private DateTime _lastPoll = DateTime.MinValue;
    private bool _initialListDone;
    private bool _disposed;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var tenantId = config[OneDriveConnectorConfig.TenantIdConfig];
        var clientId = config[OneDriveConnectorConfig.ClientIdConfig];
        var clientSecret = config[OneDriveConnectorConfig.ClientSecretConfig];

        var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
        _graphClient = new GraphServiceClient(credential);

        if (config.TryGetValue(OneDriveConnectorConfig.TopicsConfig, out var topics))
            _topic = topics.Split(',')[0].Trim();

        if (config.TryGetValue(OneDriveConnectorConfig.ModeConfig, out var mode))
            _mode = mode;

        if (config.TryGetValue(OneDriveConnectorConfig.UserIdConfig, out var userId) && !string.IsNullOrWhiteSpace(userId))
            _userId = userId;

        if (config.TryGetValue(OneDriveConnectorConfig.DriveIdConfig, out var driveId) && !string.IsNullOrWhiteSpace(driveId))
            _driveId = driveId;

        if (config.TryGetValue(OneDriveConnectorConfig.FolderPathConfig, out var folderPath))
            _folderPath = folderPath;

        if (config.TryGetValue(OneDriveConnectorConfig.FolderIdConfig, out var folderId) && !string.IsNullOrWhiteSpace(folderId))
            _folderId = folderId;

        if (config.TryGetValue(OneDriveConnectorConfig.RecursiveConfig, out var recursive))
            _recursive = bool.Parse(recursive);

        if (config.TryGetValue(OneDriveConnectorConfig.FilePatternConfig, out var pattern))
            _filePattern = pattern;

        if (config.TryGetValue(OneDriveConnectorConfig.PollIntervalMsConfig, out var pollInterval))
            _pollIntervalMs = int.Parse(pollInterval);

        if (config.TryGetValue(OneDriveConnectorConfig.UseDeltaQueryConfig, out var useDelta))
            _useDeltaQuery = bool.Parse(useDelta);

        if (config.TryGetValue(OneDriveConnectorConfig.IncludeContentConfig, out var includeContent))
            _includeContent = bool.Parse(includeContent);

        if (config.TryGetValue(OneDriveConnectorConfig.MaxFileSizeBytesConfig, out var maxSize))
            _maxFileSizeBytes = long.Parse(maxSize);

        if (config.TryGetValue(OneDriveConnectorConfig.OutputFormatConfig, out var outputFormat))
            _outputFormat = outputFormat;

        if (config.TryGetValue(OneDriveConnectorConfig.BatchSizeConfig, out var batchSize))
            _batchSize = int.Parse(batchSize);

        if (config.TryGetValue(OneDriveConnectorConfig.RetryMaxConfig, out var retryMax))
            _retryMax = int.Parse(retryMax);

        if (config.TryGetValue(OneDriveConnectorConfig.RetryBackoffMsConfig, out var retryBackoff))
            _retryBackoffMs = int.Parse(retryBackoff);
    }

    public override void Stop()
    {
        _graphClient?.Dispose();
        _graphClient = null;
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        if (_graphClient == null) return [];

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
                OneDriveConnectorConfig.ModeSourceList => await ListFilesAsync(cancellationToken),
                OneDriveConnectorConfig.ModeSourceDelta when _useDeltaQuery => await DeltaQueryAsync(cancellationToken),
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
        if (_graphClient == null) return [];

        var records = new List<SourceRecord>();

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var children = await GetDriveItemChildren(cancellationToken);
                if (children?.Value == null) break;

                foreach (var item in children.Value.Take(_batchSize))
                {
                    if (item.File == null) continue; // Skip folders
                    if (!MatchesPattern(item.Name)) continue;

                    var record = await CreateSourceRecordAsync(item, "list", cancellationToken);
                    if (record != null)
                        records.Add(record);
                }

                _initialListDone = true;
                break;
            }
            catch (global::Microsoft.Graph.Models.ODataErrors.ODataError) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
            }
        }

        return records;
    }

    private async Task<IReadOnlyList<SourceRecord>> DeltaQueryAsync(CancellationToken cancellationToken)
    {
        if (_graphClient == null) return [];

        var records = new List<SourceRecord>();

        // If no delta link, do initial list
        if (_deltaLink == null && !_initialListDone)
        {
            return await ListFilesAsync(cancellationToken);
        }

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                DeltaGetResponse? deltaResponse;

                if (_deltaLink != null)
                {
                    // Use delta link from previous call
                    deltaResponse = await _graphClient.Drives[GetDriveId()].Items["root"].Delta
                        .WithUrl(_deltaLink)
                        .GetAsDeltaGetResponseAsync(cancellationToken: cancellationToken);
                }
                else
                {
                    // Initial delta query
                    deltaResponse = await _graphClient.Drives[GetDriveId()].Items["root"].Delta
                        .GetAsDeltaGetResponseAsync(cancellationToken: cancellationToken);
                }

                if (deltaResponse == null) break;

                foreach (var item in deltaResponse.Value?.Take(_batchSize) ?? [])
                {
                    if (item.Deleted != null)
                    {
                        // Item was deleted
                        var removeRecord = CreateRemoveRecord(item.Id ?? "");
                        records.Add(removeRecord);
                    }
                    else if (item.File != null)
                    {
                        if (!MatchesPattern(item.Name)) continue;

                        var record = await CreateSourceRecordAsync(item, "delta", cancellationToken);
                        if (record != null)
                            records.Add(record);
                    }
                }

                // Store the delta link for next call
                _deltaLink = deltaResponse.OdataNextLink ?? deltaResponse.OdataDeltaLink;
                break;
            }
            catch (global::Microsoft.Graph.Models.ODataErrors.ODataError) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
            }
        }

        return records;
    }

    private async Task<DriveItemCollectionResponse?> GetDriveItemChildren(CancellationToken cancellationToken)
    {
        if (_graphClient == null) return null;

        var driveId = GetDriveId();

        if (!string.IsNullOrEmpty(_folderId))
        {
            return await _graphClient.Drives[driveId].Items[_folderId].Children
                .GetAsync(r => r.QueryParameters.Top = _batchSize, cancellationToken);
        }
        else if (_folderPath == "/" || string.IsNullOrEmpty(_folderPath))
        {
            // For root folder, use Items["root"].Children
            return await _graphClient.Drives[driveId].Items["root"].Children
                .GetAsync(r => r.QueryParameters.Top = _batchSize, cancellationToken);
        }
        else
        {
            // Navigate by path
            var path = _folderPath.TrimStart('/');
            return await _graphClient.Drives[driveId].Root.ItemWithPath(path).Children
                .GetAsync(r => r.QueryParameters.Top = _batchSize, cancellationToken);
        }
    }

    private string GetDriveId()
    {
        // If explicit drive ID is specified, use it
        if (!string.IsNullOrEmpty(_driveId))
            return _driveId;

        // Otherwise, we'll use the default "me" drive for user context
        // Note: For app-only auth, you need to specify a drive ID
        return _driveId ?? "me";
    }

    private bool MatchesPattern(string? fileName)
    {
        if (string.IsNullOrEmpty(fileName)) return false;
        if (_filePattern == "*") return true;

        var regex = "^" + Regex.Escape(_filePattern).Replace("\\*", ".*").Replace("\\?", ".") + "$";
        return Regex.IsMatch(fileName, regex, RegexOptions.IgnoreCase);
    }

    private async Task<SourceRecord?> CreateSourceRecordAsync(DriveItem item, string eventType, CancellationToken cancellationToken)
    {
        if (_graphClient == null || item.Id == null) return null;

        byte[]? content = null;

        if (_includeContent && item.Size.HasValue && item.Size.Value <= _maxFileSizeBytes)
        {
            try
            {
                var driveId = GetDriveId();
                var stream = await _graphClient.Drives[driveId].Items[item.Id].Content
                    .GetAsync(cancellationToken: cancellationToken);

                if (stream != null)
                {
                    using var ms = new MemoryStream();
                    await stream.CopyToAsync(ms, cancellationToken);
                    content = ms.ToArray();
                }
            }
            catch
            {
                // Ignore content download errors
            }
        }

        byte[] value;

        if (_outputFormat == OneDriveConnectorConfig.FormatBytes && content != null)
        {
            value = content;
        }
        else
        {
            var json = new JsonObject
            {
                ["id"] = item.Id,
                ["name"] = item.Name,
                ["mimeType"] = item.File?.MimeType,
                ["size"] = item.Size,
                ["createdDateTime"] = item.CreatedDateTime?.ToString("O"),
                ["lastModifiedDateTime"] = item.LastModifiedDateTime?.ToString("O"),
                ["webUrl"] = item.WebUrl,
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
            Key = Encoding.UTF8.GetBytes(item.Id),
            Value = value,
            Timestamp = item.LastModifiedDateTime ?? DateTimeOffset.UtcNow,
            SourcePartition = new Dictionary<string, object> { ["driveId"] = _driveId ?? "me" },
            SourceOffset = new Dictionary<string, object> { ["itemId"] = item.Id, ["lastModified"] = item.LastModifiedDateTime?.ToString("O") ?? "" }
        };
    }

    private SourceRecord CreateRemoveRecord(string itemId)
    {
        var json = new JsonObject
        {
            ["id"] = itemId,
            ["eventType"] = "deleted"
        };

        return new SourceRecord
        {
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(itemId),
            Value = Encoding.UTF8.GetBytes(json.ToJsonString(SerializerOptions)),
            Timestamp = DateTimeOffset.UtcNow,
            SourcePartition = new Dictionary<string, object> { ["driveId"] = _driveId ?? "me" },
            SourceOffset = new Dictionary<string, object> { ["itemId"] = itemId, ["deleted"] = true }
        };
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _graphClient?.Dispose();
                _graphClient = null;
            }
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}
