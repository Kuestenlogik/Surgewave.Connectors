using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Google.Photos;

/// <summary>
/// Source connector that watches Google Photos albums for new media items.
/// </summary>
[ConnectorMetadata(
    Name = "google-photos-source",
    Description = "Watches Google Photos albums for new photos and videos",
    Author = "Surgewave",
    Tags = "google, photos, media, source, cloud")]
public sealed class GooglePhotosSourceConnector : SourceConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(GooglePhotosConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Surgewave topic to produce media items to", EditorHint.Topic)
        .Define(GooglePhotosConnectorConfig.CredentialsJson, ConfigType.String, "", Importance.High,
            "Google service account credentials JSON (inline)")
        .Define(GooglePhotosConnectorConfig.CredentialsFile, ConfigType.String, "", Importance.High,
            "Path to Google service account credentials JSON file", EditorHint.FilePath)
        .Define(GooglePhotosConnectorConfig.ClientId, ConfigType.String, "", Importance.High,
            "OAuth2 client ID")
        .Define(GooglePhotosConnectorConfig.ClientSecret, ConfigType.Password, "", Importance.High,
            "OAuth2 client secret")
        .Define(GooglePhotosConnectorConfig.RefreshToken, ConfigType.Password, "", Importance.High,
            "OAuth2 refresh token")
        .Define(GooglePhotosConnectorConfig.Albums, ConfigType.List, "", Importance.Medium,
            "Comma-separated list of album names to watch")
        .Define(GooglePhotosConnectorConfig.AlbumId, ConfigType.String, "", Importance.Medium,
            "Specific album ID to watch")
        .Define(GooglePhotosConnectorConfig.MediaTypes, ConfigType.String, GooglePhotosConnectorConfig.DefaultMediaTypes,
            Importance.Medium, "Media types to include: photo, video, all")
        .Define(GooglePhotosConnectorConfig.PollIntervalMs, ConfigType.Int,
            GooglePhotosConnectorConfig.DefaultPollIntervalMs.ToString(), Importance.Medium,
            "Poll interval in milliseconds")
        .Define(GooglePhotosConnectorConfig.IncludeShared, ConfigType.Boolean, "false", Importance.Low,
            "Include shared albums")
        .Define(GooglePhotosConnectorConfig.DateRangeStart, ConfigType.String, "", Importance.Low,
            "Filter by date range start (ISO 8601)")
        .Define(GooglePhotosConnectorConfig.DateRangeEnd, ConfigType.String, "", Importance.Low,
            "Filter by date range end (ISO 8601)")
        .Define(GooglePhotosConnectorConfig.IncludeMetadata, ConfigType.Boolean, "true", Importance.Medium,
            "Include media metadata in records")
        .Define(GooglePhotosConnectorConfig.IncludeContent, ConfigType.Boolean, "false", Importance.Medium,
            "Include actual media content (may be large)")
        .Define(GooglePhotosConnectorConfig.ContentMaxSize, ConfigType.Int,
            GooglePhotosConnectorConfig.DefaultContentMaxSize.ToString(), Importance.Low,
            "Maximum content size to include in bytes");

    public override Type TaskClass => typeof(GooglePhotosSourceTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(GooglePhotosConnectorConfig.Topic, out var topic) ||
            string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException($"'{GooglePhotosConnectorConfig.Topic}' is required");
        }

        // Validate authentication
        var hasCredentialsJson = config.TryGetValue(GooglePhotosConnectorConfig.CredentialsJson, out var json) && !string.IsNullOrWhiteSpace(json);
        var hasCredentialsFile = config.TryGetValue(GooglePhotosConnectorConfig.CredentialsFile, out var file) && !string.IsNullOrWhiteSpace(file);
        var hasOAuth = config.TryGetValue(GooglePhotosConnectorConfig.ClientId, out var clientId) && !string.IsNullOrWhiteSpace(clientId) &&
                       config.TryGetValue(GooglePhotosConnectorConfig.RefreshToken, out var token) && !string.IsNullOrWhiteSpace(token);

        if (!hasCredentialsJson && !hasCredentialsFile && !hasOAuth)
        {
            throw new ArgumentException("Authentication required: provide credentials JSON, credentials file, or OAuth2 client ID with refresh token");
        }
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        return [new Dictionary<string, string>(_config)];
    }

    public override void Stop()
    {
    }
}
