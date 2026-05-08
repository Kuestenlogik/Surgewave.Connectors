namespace Kuestenlogik.Surgewave.Connector.Google.Drive;

using System.Text.Json;
using System.Text.Json.Nodes;
using global::Google.Apis.Auth.OAuth2;
using global::Google.Apis.Drive.v3;
using global::Google.Apis.Services;
using global::Google.Apis.Upload;
using Kuestenlogik.Surgewave.Connect;
using File = global::Google.Apis.Drive.v3.Data.File;

/// <summary>
/// A sink task that writes files to Google Drive.
/// Supports uploading and updating files.
/// </summary>
public sealed class GoogleDriveSinkTask : SinkTask
{
    private DriveService? _driveService;
    private string _uploadFolderId = GoogleDriveConnectorConfig.DefaultFolderId;
    private string _fileNameField = GoogleDriveConnectorConfig.DefaultFileNameField;
    private string _contentField = GoogleDriveConnectorConfig.DefaultContentField;
    private string _mimeTypeField = GoogleDriveConnectorConfig.DefaultMimeTypeField;
    private string _updateMode = GoogleDriveConnectorConfig.DefaultUpdateMode;
    private int _batchSize = GoogleDriveConnectorConfig.DefaultBatchSize;
    private int _retryMax = GoogleDriveConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = GoogleDriveConnectorConfig.DefaultRetryBackoffMs;
    private bool _disposed;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Initialize Google Drive service
        GoogleCredential credential;

#pragma warning disable CS0618 // GoogleCredential.FromJson/FromStream are deprecated but still functional
        if (config.TryGetValue(GoogleDriveConnectorConfig.CredentialsJsonConfig, out var credJson) && !string.IsNullOrWhiteSpace(credJson))
        {
            credential = GoogleCredential.FromJson(credJson).CreateScoped(DriveService.Scope.Drive);
        }
        else if (config.TryGetValue(GoogleDriveConnectorConfig.CredentialsFileConfig, out var credFile) && !string.IsNullOrWhiteSpace(credFile))
        {
            using var stream = new FileStream(credFile, FileMode.Open, FileAccess.Read);
            credential = GoogleCredential.FromStream(stream).CreateScoped(DriveService.Scope.Drive);
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

        if (config.TryGetValue(GoogleDriveConnectorConfig.UploadFolderIdConfig, out var folderId))
            _uploadFolderId = folderId;

        if (config.TryGetValue(GoogleDriveConnectorConfig.FileNameFieldConfig, out var fileNameField))
            _fileNameField = fileNameField;

        if (config.TryGetValue(GoogleDriveConnectorConfig.ContentFieldConfig, out var contentField))
            _contentField = contentField;

        if (config.TryGetValue(GoogleDriveConnectorConfig.MimeTypeFieldConfig, out var mimeTypeField))
            _mimeTypeField = mimeTypeField;

        if (config.TryGetValue(GoogleDriveConnectorConfig.UpdateModeConfig, out var updateMode))
            _updateMode = updateMode;

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

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0 || _driveService == null) return;

        // Process in batches
        for (var i = 0; i < records.Count; i += _batchSize)
        {
            var batch = records.Skip(i).Take(_batchSize).ToList();
            await ProcessBatchAsync(batch, cancellationToken);
        }
    }

    private async Task ProcessBatchAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            await ProcessRecordAsync(record, cancellationToken);
        }
    }

    private async Task ProcessRecordAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        if (_driveService == null || record.Value == null) return;

        try
        {
            var json = JsonSerializer.Deserialize<JsonObject>(record.Value);
            if (json == null) return;

            var fileName = json[_fileNameField]?.GetValue<string>();
            if (string.IsNullOrWhiteSpace(fileName)) return;

            var contentStr = json[_contentField]?.GetValue<string>();
            if (string.IsNullOrWhiteSpace(contentStr)) return;

            var mimeType = json[_mimeTypeField]?.GetValue<string>() ?? GoogleDriveConnectorConfig.DefaultMimeType;

            // Decode content (assume base64)
            byte[] content;
            try
            {
                content = Convert.FromBase64String(contentStr);
            }
            catch
            {
                // If not base64, use as UTF-8
                content = System.Text.Encoding.UTF8.GetBytes(contentStr);
            }

            await UploadFileAsync(fileName, content, mimeType, cancellationToken);
        }
        catch (JsonException)
        {
            // Invalid JSON, skip record
        }
    }

    private async Task UploadFileAsync(string fileName, byte[] content, string mimeType, CancellationToken cancellationToken)
    {
        if (_driveService == null) return;

        // Check if file exists (for replace/create-or-replace modes)
        string? existingFileId = null;

        if (_updateMode != GoogleDriveConnectorConfig.UpdateModeCreate)
        {
            existingFileId = await FindExistingFileAsync(fileName, cancellationToken);
        }

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                if (existingFileId != null && _updateMode != GoogleDriveConnectorConfig.UpdateModeCreate)
                {
                    // Update existing file
                    var fileMetadata = new File { Name = fileName };

                    using var stream = new MemoryStream(content);
                    var request = _driveService.Files.Update(fileMetadata, existingFileId, stream, mimeType);
                    request.Fields = "id, name";

                    var result = await request.UploadAsync(cancellationToken);
                    if (result.Status == UploadStatus.Failed)
                        throw result.Exception ?? new Exception("Upload failed");
                }
                else
                {
                    // Create new file
                    var fileMetadata = new File
                    {
                        Name = fileName,
                        Parents = _uploadFolderId != "root" ? [_uploadFolderId] : null
                    };

                    using var stream = new MemoryStream(content);
                    var request = _driveService.Files.Create(fileMetadata, stream, mimeType);
                    request.Fields = "id, name";

                    var result = await request.UploadAsync(cancellationToken);
                    if (result.Status == UploadStatus.Failed)
                        throw result.Exception ?? new Exception("Upload failed");
                }

                break;
            }
            catch (global::Google.GoogleApiException) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
            }
        }
    }

    private async Task<string?> FindExistingFileAsync(string fileName, CancellationToken cancellationToken)
    {
        if (_driveService == null) return null;

        var query = $"name = '{fileName.Replace("'", "\\'")}' and trashed = false";
        if (_uploadFolderId != "root")
        {
            query += $" and '{_uploadFolderId}' in parents";
        }

        var request = _driveService.Files.List();
        request.Q = query;
        request.Fields = "files(id)";
        request.PageSize = 1;

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var result = await request.ExecuteAsync(cancellationToken);
                return result.Files?.FirstOrDefault()?.Id;
            }
            catch (global::Google.GoogleApiException) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
            }
        }

        return null;
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
