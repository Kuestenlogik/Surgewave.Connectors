namespace Kuestenlogik.Surgewave.Connector.Google.Drive;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that writes files to Google Drive.
/// Supports uploading and updating files.
/// </summary>
public sealed class GoogleDriveSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(GoogleDriveSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Authentication
        .Define(GoogleDriveConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.High,
            "Google service account credentials JSON (inline)")
        .Define(GoogleDriveConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.High,
            "Path to Google service account credentials JSON file", EditorHint.FilePath)
        // Topics
        .Define(GoogleDriveConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to consume", EditorHint.Topic)
        // Mode
        .Define(GoogleDriveConnectorConfig.ModeConfig, ConfigType.String, GoogleDriveConnectorConfig.ModeSinkUpload, Importance.High,
            "Sink mode: 'sink-upload' or 'sink-update'")
        // Upload folder
        .Define(GoogleDriveConnectorConfig.UploadFolderIdConfig, ConfigType.String, GoogleDriveConnectorConfig.DefaultFolderId, Importance.Medium,
            "Google Drive folder ID to upload files to")
        // Field mapping
        .Define(GoogleDriveConnectorConfig.FileNameFieldConfig, ConfigType.String, GoogleDriveConnectorConfig.DefaultFileNameField, Importance.Medium,
            "JSON field containing the file name")
        .Define(GoogleDriveConnectorConfig.ContentFieldConfig, ConfigType.String, GoogleDriveConnectorConfig.DefaultContentField, Importance.Medium,
            "JSON field containing the file content (base64 encoded or raw)")
        .Define(GoogleDriveConnectorConfig.MimeTypeFieldConfig, ConfigType.String, GoogleDriveConnectorConfig.DefaultMimeTypeField, Importance.Low,
            "JSON field containing the MIME type")
        // Update behavior
        .Define(GoogleDriveConnectorConfig.UpdateModeConfig, ConfigType.String, GoogleDriveConnectorConfig.DefaultUpdateMode, Importance.Medium,
            "Update mode: 'create' (always new), 'replace' (update existing), 'create-or-replace'")
        // Batching
        .Define(GoogleDriveConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)GoogleDriveConnectorConfig.DefaultBatchSize, Importance.Low,
            "Number of files to batch in each upload")
        // Retry
        .Define(GoogleDriveConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)GoogleDriveConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(GoogleDriveConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)GoogleDriveConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate credentials
        var hasCredentialsJson = config.TryGetValue(GoogleDriveConnectorConfig.CredentialsJsonConfig, out var credJson)
            && !string.IsNullOrWhiteSpace(credJson);
        var hasCredentialsFile = config.TryGetValue(GoogleDriveConnectorConfig.CredentialsFileConfig, out var credFile)
            && !string.IsNullOrWhiteSpace(credFile);

        if (!hasCredentialsJson && !hasCredentialsFile)
            throw new ArgumentException($"Missing credentials: provide either {GoogleDriveConnectorConfig.CredentialsJsonConfig} or {GoogleDriveConnectorConfig.CredentialsFileConfig}");

        // Validate topics
        if (!config.TryGetValue(GoogleDriveConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {GoogleDriveConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(GoogleDriveConnectorConfig.ModeConfig, out var m)
            ? m
            : GoogleDriveConnectorConfig.ModeSinkUpload;

        var validModes = new[]
        {
            GoogleDriveConnectorConfig.ModeSinkUpload,
            GoogleDriveConnectorConfig.ModeSinkUpdate
        };

        if (!validModes.Contains(mode))
            throw new ArgumentException($"Invalid mode: {mode}. Must be one of: {string.Join(", ", validModes)}");

        // Validate update mode
        if (config.TryGetValue(GoogleDriveConnectorConfig.UpdateModeConfig, out var updateMode))
        {
            var validUpdateModes = new[]
            {
                GoogleDriveConnectorConfig.UpdateModeCreate,
                GoogleDriveConnectorConfig.UpdateModeReplace,
                GoogleDriveConnectorConfig.UpdateModeCreateOrReplace
            };

            if (!validUpdateModes.Contains(updateMode))
                throw new ArgumentException($"Invalid update mode: {updateMode}. Must be one of: {string.Join(", ", validUpdateModes)}");
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
