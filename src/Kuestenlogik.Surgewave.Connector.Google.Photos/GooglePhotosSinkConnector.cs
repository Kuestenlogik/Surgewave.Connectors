using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.Google.Photos;

/// <summary>
/// Sink connector that uploads media items to Google Photos.
/// </summary>
[ConnectorMetadata(
    Name = "google-photos-sink",
    Description = "Uploads media items to Google Photos albums",
    Author = "Surgewave",
    Tags = "google, photos, media, sink, cloud, upload")]
public sealed class GooglePhotosSinkConnector : SinkConnector
{
    private IDictionary<string, string> _config = null!;

    public override string Version => "1.0.0";

    public override ConfigDef Config => new ConfigDef()
        .Define(GooglePhotosConnectorConfig.Topics, ConfigType.List, Importance.High,
            "Surgewave topics to consume media from", EditorHint.Topic)
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
        .Define(GooglePhotosConnectorConfig.UploadAlbumId, ConfigType.String, "", Importance.Medium,
            "Album ID to upload media to")
        .Define(GooglePhotosConnectorConfig.CreateAlbum, ConfigType.Boolean, "false", Importance.Medium,
            "Create album if it doesn't exist")
        .Define(GooglePhotosConnectorConfig.AlbumTitle, ConfigType.String, "", Importance.Medium,
            "Title for new album (if creating)");

    public override Type TaskClass => typeof(GooglePhotosSinkTask);

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        if (!config.TryGetValue(GooglePhotosConnectorConfig.Topics, out var topics) ||
            string.IsNullOrWhiteSpace(topics))
        {
            throw new ArgumentException($"'{GooglePhotosConnectorConfig.Topics}' is required");
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
