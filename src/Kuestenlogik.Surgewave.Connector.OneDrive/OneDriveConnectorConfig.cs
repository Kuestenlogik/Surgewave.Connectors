namespace Kuestenlogik.Surgewave.Connector.OneDrive;

/// <summary>
/// Configuration constants for the OneDrive connector.
/// </summary>
public static class OneDriveConnectorConfig
{
    // Topics
    public const string TopicsConfig = "topics";

    // Authentication (Azure AD App Registration)
    public const string TenantIdConfig = "azure.tenant.id";
    public const string ClientIdConfig = "azure.client.id";
    public const string ClientSecretConfig = "azure.client.secret";

    // User configuration
    public const string UserIdConfig = "user.id";
    public const string DriveIdConfig = "drive.id";

    // Mode configuration
    public const string ModeConfig = "mode";
    public const string ModeSourceDelta = "source-delta";
    public const string ModeSourceList = "source-list";
    public const string ModeSinkUpload = "sink-upload";
    public const string ModeSinkUpdate = "sink-update";

    // Folder configuration
    public const string FolderPathConfig = "folder.path";
    public const string DefaultFolderPath = "/";
    public const string FolderIdConfig = "folder.id";
    public const string RecursiveConfig = "recursive";
    public const bool DefaultRecursive = false;

    // File filtering
    public const string FilePatternConfig = "file.pattern";
    public const string DefaultFilePattern = "*";

    // Polling configuration (for source)
    public const string PollIntervalMsConfig = "poll.interval.ms";
    public const int DefaultPollIntervalMs = 30000;
    public const string UseDeltaQueryConfig = "use.delta.query";
    public const bool DefaultUseDeltaQuery = true;

    // Upload configuration (for sink)
    public const string UploadFolderPathConfig = "upload.folder.path";
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

    // Conflict behavior
    public const string ConflictBehaviorConfig = "conflict.behavior";
    public const string ConflictBehaviorRename = "rename";
    public const string ConflictBehaviorReplace = "replace";
    public const string ConflictBehaviorFail = "fail";
    public const string DefaultConflictBehavior = ConflictBehaviorReplace;

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
