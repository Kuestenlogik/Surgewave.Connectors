namespace Kuestenlogik.Surgewave.Connector.Google.Drive;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that reads files from Google Drive.
/// Supports watching for changes and listing files.
/// </summary>
public sealed class GoogleDriveSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(GoogleDriveSourceTask);

    public override ConfigDef Config => new ConfigDef()
        // Authentication
        .Define(GoogleDriveConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.High,
            "Google service account credentials JSON (inline)")
        .Define(GoogleDriveConnectorConfig.CredentialsFileConfig, ConfigType.String, "", Importance.High,
            "Path to Google service account credentials JSON file", EditorHint.FilePath)
        // Topics
        .Define(GoogleDriveConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to produce to", EditorHint.Topic)
        // Mode
        .Define(GoogleDriveConnectorConfig.ModeConfig, ConfigType.String, GoogleDriveConnectorConfig.ModeSourceWatch, Importance.High,
            "Source mode: 'source-watch' (poll for changes), 'source-list' (list files once)")
        // Folder configuration
        .Define(GoogleDriveConnectorConfig.FolderIdConfig, ConfigType.String, GoogleDriveConnectorConfig.DefaultFolderId, Importance.Medium,
            "Google Drive folder ID to watch (use 'root' for root folder)")
        .Define(GoogleDriveConnectorConfig.RecursiveConfig, ConfigType.Boolean, GoogleDriveConnectorConfig.DefaultRecursive, Importance.Low,
            "Recursively watch subfolders")
        // File filtering
        .Define(GoogleDriveConnectorConfig.FilePatternConfig, ConfigType.String, GoogleDriveConnectorConfig.DefaultFilePattern, Importance.Low,
            "File name pattern to match (glob-style)")
        .Define(GoogleDriveConnectorConfig.MimeTypeFilterConfig, ConfigType.String, "", Importance.Low,
            "Comma-separated MIME types to include (empty for all)")
        // Polling
        .Define(GoogleDriveConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (long)GoogleDriveConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(GoogleDriveConnectorConfig.TrackChangesConfig, ConfigType.Boolean, GoogleDriveConnectorConfig.DefaultTrackChanges, Importance.Medium,
            "Use Google Drive Changes API for incremental updates")
        // Content handling
        .Define(GoogleDriveConnectorConfig.IncludeContentConfig, ConfigType.Boolean, GoogleDriveConnectorConfig.DefaultIncludeContent, Importance.Medium,
            "Include file content in messages")
        .Define(GoogleDriveConnectorConfig.MaxFileSizeBytesConfig, ConfigType.Int, GoogleDriveConnectorConfig.DefaultMaxFileSizeBytes, Importance.Low,
            "Maximum file size to include content (bytes)")
        // Output format
        .Define(GoogleDriveConnectorConfig.OutputFormatConfig, ConfigType.String, GoogleDriveConnectorConfig.DefaultOutputFormat, Importance.Low,
            "Output format: 'json' (metadata + optional content) or 'bytes' (raw content)")
        // Batching
        .Define(GoogleDriveConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)GoogleDriveConnectorConfig.DefaultBatchSize, Importance.Low,
            "Number of files to batch in each poll")
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
            : GoogleDriveConnectorConfig.ModeSourceWatch;

        var validModes = new[]
        {
            GoogleDriveConnectorConfig.ModeSourceWatch,
            GoogleDriveConnectorConfig.ModeSourceList
        };

        if (!validModes.Contains(mode))
            throw new ArgumentException($"Invalid mode: {mode}. Must be one of: {string.Join(", ", validModes)}");

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
