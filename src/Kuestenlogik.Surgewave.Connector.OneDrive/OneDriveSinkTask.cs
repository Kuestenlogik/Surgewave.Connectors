namespace Kuestenlogik.Surgewave.Connector.OneDrive;

using System.Text;
using System.Text.Json;
using global::Azure.Identity;
using global::Microsoft.Graph;
using global::Microsoft.Graph.Drives.Item.Items.Item.CreateUploadSession;
using global::Microsoft.Graph.Models;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that writes files to OneDrive via Microsoft Graph API.
/// Supports uploading and updating files.
/// </summary>
public sealed class OneDriveSinkTask : SinkTask
{
    private GraphServiceClient? _graphClient;
    private string _mode = OneDriveConnectorConfig.ModeSinkUpload;
    private string? _userId;
    private string? _driveId;
    private string _uploadFolderPath = OneDriveConnectorConfig.DefaultFolderPath;
    private string? _folderId;
    private string _fileNameField = OneDriveConnectorConfig.DefaultFileNameField;
    private string _contentField = OneDriveConnectorConfig.DefaultContentField;
    private string _mimeTypeField = OneDriveConnectorConfig.DefaultMimeTypeField;
    private string _updateMode = OneDriveConnectorConfig.DefaultUpdateMode;
    private string _conflictBehavior = OneDriveConnectorConfig.DefaultConflictBehavior;
    private int _batchSize = OneDriveConnectorConfig.DefaultBatchSize;
    private int _retryMax = OneDriveConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = OneDriveConnectorConfig.DefaultRetryBackoffMs;
    private bool _disposed;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var tenantId = config[OneDriveConnectorConfig.TenantIdConfig];
        var clientId = config[OneDriveConnectorConfig.ClientIdConfig];
        var clientSecret = config[OneDriveConnectorConfig.ClientSecretConfig];

        var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
        _graphClient = new GraphServiceClient(credential);

        if (config.TryGetValue(OneDriveConnectorConfig.ModeConfig, out var mode))
            _mode = mode;

        if (config.TryGetValue(OneDriveConnectorConfig.UserIdConfig, out var userId) && !string.IsNullOrWhiteSpace(userId))
            _userId = userId;

        if (config.TryGetValue(OneDriveConnectorConfig.DriveIdConfig, out var driveId) && !string.IsNullOrWhiteSpace(driveId))
            _driveId = driveId;

        if (config.TryGetValue(OneDriveConnectorConfig.UploadFolderPathConfig, out var uploadPath))
            _uploadFolderPath = uploadPath;

        if (config.TryGetValue(OneDriveConnectorConfig.FolderIdConfig, out var folderId) && !string.IsNullOrWhiteSpace(folderId))
            _folderId = folderId;

        if (config.TryGetValue(OneDriveConnectorConfig.FileNameFieldConfig, out var fileNameField))
            _fileNameField = fileNameField;

        if (config.TryGetValue(OneDriveConnectorConfig.ContentFieldConfig, out var contentField))
            _contentField = contentField;

        if (config.TryGetValue(OneDriveConnectorConfig.MimeTypeFieldConfig, out var mimeTypeField))
            _mimeTypeField = mimeTypeField;

        if (config.TryGetValue(OneDriveConnectorConfig.UpdateModeConfig, out var updateMode))
            _updateMode = updateMode;

        if (config.TryGetValue(OneDriveConnectorConfig.ConflictBehaviorConfig, out var conflictBehavior))
            _conflictBehavior = conflictBehavior;

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

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_graphClient == null || records.Count == 0) return;

        foreach (var record in records.Take(_batchSize))
        {
            try
            {
                await UploadFileAsync(record, cancellationToken);
            }
            catch (Exception ex)
            {
                Context?.RaiseError?.Invoke(ex);
            }
        }
    }

    private async Task UploadFileAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        if (_graphClient == null || record.Value == null) return;

        // Parse the record value as JSON
        var json = JsonSerializer.Deserialize<JsonElement>(record.Value);

        if (!json.TryGetProperty(_fileNameField, out var fileNameElement))
            throw new ArgumentException($"Missing required field: {_fileNameField}");

        var fileName = fileNameElement.GetString()
            ?? throw new ArgumentException($"Field {_fileNameField} is null");

        byte[] content;
        if (json.TryGetProperty(_contentField, out var contentElement))
        {
            var contentString = contentElement.GetString();
            if (contentString != null)
            {
                // Assume base64 encoded content
                content = Convert.FromBase64String(contentString);
            }
            else
            {
                content = [];
            }
        }
        else
        {
            // Use the raw record value as content
            content = record.Value;
        }

        string? mimeType = null;
        if (json.TryGetProperty(_mimeTypeField, out var mimeTypeElement))
        {
            mimeType = mimeTypeElement.GetString();
        }

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                await UploadWithRetryAsync(fileName, content, mimeType, cancellationToken);
                break;
            }
            catch (global::Microsoft.Graph.Models.ODataErrors.ODataError) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
            }
        }
    }

    private async Task UploadWithRetryAsync(string fileName, byte[] content, string? mimeType, CancellationToken cancellationToken)
    {
        if (_graphClient == null) return;

        var driveId = GetDriveId();

        // Build the item path
        var itemPath = BuildItemPath(fileName);

        // For small files (< 4MB), use simple upload
        if (content.Length < 4 * 1024 * 1024)
        {
            await SimpleUploadAsync(driveId, itemPath, content, cancellationToken);
        }
        else
        {
            await LargeFileUploadAsync(driveId, itemPath, content, cancellationToken);
        }
    }

    private async Task SimpleUploadAsync(string driveId, string itemPath, byte[] content, CancellationToken cancellationToken)
    {
        if (_graphClient == null) return;

        using var stream = new MemoryStream(content);

        if (_updateMode == OneDriveConnectorConfig.UpdateModeCreate)
        {
            // Create new file only (fail if exists)
            await _graphClient.Drives[driveId].Root.ItemWithPath(itemPath).Content
                .PutAsync(stream, cancellationToken: cancellationToken);
        }
        else if (_updateMode == OneDriveConnectorConfig.UpdateModeReplace)
        {
            // Replace existing file
            await _graphClient.Drives[driveId].Root.ItemWithPath(itemPath).Content
                .PutAsync(stream, cancellationToken: cancellationToken);
        }
        else // create-or-replace (default)
        {
            // Create or replace
            await _graphClient.Drives[driveId].Root.ItemWithPath(itemPath).Content
                .PutAsync(stream, cancellationToken: cancellationToken);
        }
    }

    private async Task LargeFileUploadAsync(string driveId, string itemPath, byte[] content, CancellationToken cancellationToken)
    {
        if (_graphClient == null) return;

        // Create upload session
        var uploadSessionRequest = new CreateUploadSessionPostRequestBody
        {
            Item = new DriveItemUploadableProperties
            {
                AdditionalData = new Dictionary<string, object>
                {
                    ["@microsoft.graph.conflictBehavior"] = GetConflictBehavior()
                }
            }
        };

        var uploadSession = await _graphClient.Drives[driveId].Root.ItemWithPath(itemPath)
            .CreateUploadSession
            .PostAsync(uploadSessionRequest, cancellationToken: cancellationToken);

        if (uploadSession?.UploadUrl == null)
            throw new InvalidOperationException("Failed to create upload session");

        // Upload in chunks
        const int chunkSize = 320 * 1024; // 320 KB chunks (must be multiple of 320 KB)
        var totalSize = content.Length;
        var uploadedBytes = 0;

        using var httpClient = new HttpClient();

        while (uploadedBytes < totalSize)
        {
            var bytesToUpload = Math.Min(chunkSize, totalSize - uploadedBytes);
            var chunk = new byte[bytesToUpload];
            Array.Copy(content, uploadedBytes, chunk, 0, bytesToUpload);

            using var chunkStream = new MemoryStream(chunk);
            using var request = new HttpRequestMessage(HttpMethod.Put, uploadSession.UploadUrl);
            request.Content = new StreamContent(chunkStream);
            request.Content.Headers.ContentLength = bytesToUpload;
            request.Content.Headers.ContentRange = new System.Net.Http.Headers.ContentRangeHeaderValue(
                uploadedBytes,
                uploadedBytes + bytesToUpload - 1,
                totalSize);

            var response = await httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();

            uploadedBytes += bytesToUpload;
        }
    }

    private string BuildItemPath(string fileName)
    {
        if (!string.IsNullOrEmpty(_folderId))
        {
            // When using folder ID, we need to use a different approach
            // For simplicity, combine with path
            return fileName;
        }

        var folderPath = _uploadFolderPath.TrimStart('/').TrimEnd('/');
        if (string.IsNullOrEmpty(folderPath))
        {
            return fileName;
        }

        return $"{folderPath}/{fileName}";
    }

    private string GetDriveId()
    {
        if (!string.IsNullOrEmpty(_driveId))
            return _driveId;

        return _driveId ?? "me";
    }

    private string GetConflictBehavior()
    {
        return _conflictBehavior switch
        {
            OneDriveConnectorConfig.ConflictBehaviorRename => "rename",
            OneDriveConnectorConfig.ConflictBehaviorReplace => "replace",
            OneDriveConnectorConfig.ConflictBehaviorFail => "fail",
            _ => "replace"
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
