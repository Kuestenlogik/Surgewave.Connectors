namespace Kuestenlogik.Surgewave.Connector.OneDrive;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that reads files from OneDrive via Microsoft Graph API.
/// Supports delta queries for efficient change tracking.
/// </summary>
public sealed class OneDriveSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(OneDriveSourceTask);

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
            "User ID or UPN (leave empty for app-only access to SharePoint)")
        .Define(OneDriveConnectorConfig.DriveIdConfig, ConfigType.String, "", Importance.Medium,
            "Drive ID (optional, uses default drive if not specified)")
        // Topics
        .Define(OneDriveConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of topics to produce to", EditorHint.Topic)
        // Mode
        .Define(OneDriveConnectorConfig.ModeConfig, ConfigType.String, OneDriveConnectorConfig.ModeSourceDelta, Importance.High,
            "Source mode: 'source-delta' (track changes), 'source-list' (list files)")
        // Folder configuration
        .Define(OneDriveConnectorConfig.FolderPathConfig, ConfigType.String, OneDriveConnectorConfig.DefaultFolderPath, Importance.Medium,
            "OneDrive folder path to watch")
        .Define(OneDriveConnectorConfig.FolderIdConfig, ConfigType.String, "", Importance.Medium,
            "OneDrive folder ID (alternative to path)")
        .Define(OneDriveConnectorConfig.RecursiveConfig, ConfigType.Boolean, OneDriveConnectorConfig.DefaultRecursive, Importance.Low,
            "Recursively watch subfolders")
        // File filtering
        .Define(OneDriveConnectorConfig.FilePatternConfig, ConfigType.String, OneDriveConnectorConfig.DefaultFilePattern, Importance.Low,
            "File name pattern to match (glob-style)")
        // Polling
        .Define(OneDriveConnectorConfig.PollIntervalMsConfig, ConfigType.Int, (long)OneDriveConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(OneDriveConnectorConfig.UseDeltaQueryConfig, ConfigType.Boolean, OneDriveConnectorConfig.DefaultUseDeltaQuery, Importance.Medium,
            "Use delta queries for efficient change tracking")
        // Content handling
        .Define(OneDriveConnectorConfig.IncludeContentConfig, ConfigType.Boolean, OneDriveConnectorConfig.DefaultIncludeContent, Importance.Medium,
            "Include file content in messages")
        .Define(OneDriveConnectorConfig.MaxFileSizeBytesConfig, ConfigType.Int, OneDriveConnectorConfig.DefaultMaxFileSizeBytes, Importance.Low,
            "Maximum file size to include content (bytes)")
        // Output format
        .Define(OneDriveConnectorConfig.OutputFormatConfig, ConfigType.String, OneDriveConnectorConfig.DefaultOutputFormat, Importance.Low,
            "Output format: 'json' (metadata + optional content) or 'bytes' (raw content)", EditorHint.Select, options: ["json", "merge"])
        // Batching
        .Define(OneDriveConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)OneDriveConnectorConfig.DefaultBatchSize, Importance.Low,
            "Number of files to batch in each poll")
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
            : OneDriveConnectorConfig.ModeSourceDelta;

        var validModes = new[]
        {
            OneDriveConnectorConfig.ModeSourceDelta,
            OneDriveConnectorConfig.ModeSourceList
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
