namespace Kuestenlogik.Surgewave.Connector.OneDrive;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that writes files to OneDrive via Microsoft Graph API.
/// Supports uploading and updating files.
/// </summary>
public sealed class OneDriveSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(OneDriveSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Authentication
        .Define(OneDriveConnectorConfig.TenantIdConfig, ConfigType.String, Importance.High,
            "Azure AD tenant ID")
        .Define(OneDriveConnectorConfig.ClientIdConfig, ConfigType.String, Importance.High,
            "Azure AD application (client) ID")
        .Define(OneDriveConnectorConfig.ClientSecretConfig, ConfigType.Password, Importance.High,
            "Azure AD client secret")
        // User/Drive
        .Define(OneDriveConnectorConfig.UserIdConfig, ConfigType.String, "", Importance.Medium,
            "User ID or UPN (leave empty for app-only access)")
        .Define(OneDriveConnectorConfig.DriveIdConfig, ConfigType.String, "", Importance.Medium,
            "Drive ID (optional, uses default drive if not specified)")
        // Topics
        .Define(OneDriveConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        // Mode
        .Define(OneDriveConnectorConfig.ModeConfig, ConfigType.String, OneDriveConnectorConfig.ModeSinkUpload, Importance.High,
            "Sink mode: 'sink-upload' or 'sink-update'")
        // Upload folder
        .Define(OneDriveConnectorConfig.UploadFolderPathConfig, ConfigType.String, OneDriveConnectorConfig.DefaultFolderPath, Importance.Medium,
            "OneDrive folder path to upload files to")
        .Define(OneDriveConnectorConfig.FolderIdConfig, ConfigType.String, "", Importance.Medium,
            "OneDrive folder ID (alternative to path)")
        // Field mapping
        .Define(OneDriveConnectorConfig.FileNameFieldConfig, ConfigType.String, OneDriveConnectorConfig.DefaultFileNameField, Importance.Medium,
            "JSON field containing the file name")
        .Define(OneDriveConnectorConfig.ContentFieldConfig, ConfigType.String, OneDriveConnectorConfig.DefaultContentField, Importance.Medium,
            "JSON field containing the file content (base64 encoded)")
        .Define(OneDriveConnectorConfig.MimeTypeFieldConfig, ConfigType.String, OneDriveConnectorConfig.DefaultMimeTypeField, Importance.Low,
            "JSON field containing the MIME type")
        // Update behavior
        .Define(OneDriveConnectorConfig.UpdateModeConfig, ConfigType.String, OneDriveConnectorConfig.DefaultUpdateMode, Importance.Medium,
            "Update mode: 'create' (always new), 'replace' (update existing), 'create-or-replace'", EditorHint.Select, options: ["replace", "update"])
        // Conflict behavior
        .Define(OneDriveConnectorConfig.ConflictBehaviorConfig, ConfigType.String, OneDriveConnectorConfig.DefaultConflictBehavior, Importance.Low,
            "Conflict behavior: 'rename', 'replace', 'fail'", EditorHint.Select, options: ["rename", "replace", "fail"])
        // Batching
        .Define(OneDriveConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)OneDriveConnectorConfig.DefaultBatchSize, Importance.Low,
            "Number of files to batch in each upload")
        // Retry
        .Define(OneDriveConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)OneDriveConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(OneDriveConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)OneDriveConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate authentication
        if (!config.TryGetValue(OneDriveConnectorConfig.TenantIdConfig, out _))
            throw new ArgumentException($"Missing required config: {OneDriveConnectorConfig.TenantIdConfig}");

        if (!config.TryGetValue(OneDriveConnectorConfig.ClientIdConfig, out _))
            throw new ArgumentException($"Missing required config: {OneDriveConnectorConfig.ClientIdConfig}");

        if (!config.TryGetValue(OneDriveConnectorConfig.ClientSecretConfig, out _))
            throw new ArgumentException($"Missing required config: {OneDriveConnectorConfig.ClientSecretConfig}");

        // Validate topics
        if (!config.TryGetValue(OneDriveConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {OneDriveConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(OneDriveConnectorConfig.ModeConfig, out var m)
            ? m
            : OneDriveConnectorConfig.ModeSinkUpload;

        var validModes = new[]
        {
            OneDriveConnectorConfig.ModeSinkUpload,
            OneDriveConnectorConfig.ModeSinkUpdate
        };

        if (!validModes.Contains(mode))
            throw new ArgumentException($"Invalid mode: {mode}. Must be one of: {string.Join(", ", validModes)}");

        // Validate update mode
        if (config.TryGetValue(OneDriveConnectorConfig.UpdateModeConfig, out var updateMode))
        {
            var validUpdateModes = new[]
            {
                OneDriveConnectorConfig.UpdateModeCreate,
                OneDriveConnectorConfig.UpdateModeReplace,
                OneDriveConnectorConfig.UpdateModeCreateOrReplace
            };

            if (!validUpdateModes.Contains(updateMode))
                throw new ArgumentException($"Invalid update mode: {updateMode}. Must be one of: {string.Join(", ", validUpdateModes)}");
        }

        // Validate conflict behavior
        if (config.TryGetValue(OneDriveConnectorConfig.ConflictBehaviorConfig, out var conflictBehavior))
        {
            var validBehaviors = new[]
            {
                OneDriveConnectorConfig.ConflictBehaviorRename,
                OneDriveConnectorConfig.ConflictBehaviorReplace,
                OneDriveConnectorConfig.ConflictBehaviorFail
            };

            if (!validBehaviors.Contains(conflictBehavior))
                throw new ArgumentException($"Invalid conflict behavior: {conflictBehavior}. Must be one of: {string.Join(", ", validBehaviors)}");
        }

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }
}
