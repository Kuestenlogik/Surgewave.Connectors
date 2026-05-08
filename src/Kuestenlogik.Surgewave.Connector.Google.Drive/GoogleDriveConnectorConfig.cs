namespace Kuestenlogik.Surgewave.Connector.Google.Drive;

/// <summary>
/// Configuration constants for the Google Drive connector.
/// </summary>
public static class GoogleDriveConnectorConfig
{
    // Topics
    public const string TopicsConfig = "topics";

    // Authentication
    public const string CredentialsJsonConfig = "google.credentials.json";
    public const string CredentialsFileConfig = "google.credentials.file";
    public const string ServiceAccountEmailConfig = "google.service.account.email";

    // Mode configuration
    public const string ModeConfig = "mode";
    public const string ModeSourceWatch = "source-watch";
    public const string ModeSourceList = "source-list";
    public const string ModeSinkUpload = "sink-upload";
    public const string ModeSinkUpdate = "sink-update";

    // Folder configuration
    public const string FolderIdConfig = "folder.id";
    public const string DefaultFolderId = "root";
    public const string RecursiveConfig = "recursive";
    public const bool DefaultRecursive = false;

    // File filtering
    public const string FilePatternConfig = "file.pattern";
    public const string DefaultFilePattern = "*";
    public const string MimeTypeFilterConfig = "mime.type.filter";

    // Polling configuration (for source)
    public const string PollIntervalMsConfig = "poll.interval.ms";
    public const int DefaultPollIntervalMs = 30000;
    public const string TrackChangesConfig = "track.changes";
    public const bool DefaultTrackChanges = true;

    // Upload configuration (for sink)
    public const string UploadFolderIdConfig = "upload.folder.id";
    public const string FileNameFieldConfig = "filename.field";
    public const string DefaultFileNameField = "filename";
    public const string ContentFieldConfig = "content.field";
    public const string DefaultContentField = "content";
    public const string MimeTypeFieldConfig = "mimetype.field";
    public const string DefaultMimeTypeField = "mimetype";
    public const string DefaultMimeType = "application/octet-stream";

    // Update behavior
    public const string UpdateModeConfig = "update.mode";
    public const string UpdateModeCreate = "create";
    public const string UpdateModeReplace = "replace";
    public const string UpdateModeCreateOrReplace = "create-or-replace";
    public const string DefaultUpdateMode = UpdateModeCreateOrReplace;

    // Content handling
    public const string IncludeContentConfig = "include.content";
    public const bool DefaultIncludeContent = false;
    public const string MaxFileSizeBytesConfig = "max.file.size.bytes";
    public const long DefaultMaxFileSizeBytes = 10 * 1024 * 1024; // 10MB

    // Output format
    public const string OutputFormatConfig = "output.format";
    public const string FormatJson = "json";
    public const string FormatBytes = "bytes";
    public const string DefaultOutputFormat = FormatJson;

    // Batching
    public const string BatchSizeConfig = "batch.size";
    public const int DefaultBatchSize = 10;

    // Retry configuration
    public const string RetryMaxConfig = "retry.max";
    public const int DefaultRetryMax = 3;
    public const string RetryBackoffMsConfig = "retry.backoff.ms";
    public const int DefaultRetryBackoffMs = 1000;
}
