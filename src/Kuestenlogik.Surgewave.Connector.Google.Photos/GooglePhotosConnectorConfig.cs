namespace Kuestenlogik.Surgewave.Connector.Google.Photos;

/// <summary>
/// Configuration constants for Google Photos connector.
/// </summary>
public static class GooglePhotosConnectorConfig
{
    // Authentication
    public const string CredentialsJson = "google.credentials.json";
    public const string CredentialsFile = "google.credentials.file";
    public const string RefreshToken = "google.refresh.token";
    public const string ClientId = "google.client.id";
    public const string ClientSecret = "google.client.secret";

    // Source settings
    public const string Topic = "topic";
    public const string Topics = "topics";
    public const string Albums = "google.photos.albums";
    public const string AlbumId = "google.photos.album.id";
    public const string MediaTypes = "google.photos.media.types";  // photo, video, all
    public const string PollIntervalMs = "poll.interval.ms";
    public const string IncludeShared = "google.photos.include.shared";
    public const string DateRangeStart = "google.photos.date.range.start";
    public const string DateRangeEnd = "google.photos.date.range.end";

    // Sink settings
    public const string UploadAlbumId = "google.photos.upload.album.id";
    public const string CreateAlbum = "google.photos.create.album";
    public const string AlbumTitle = "google.photos.album.title";

    // Content handling
    public const string IncludeMetadata = "include.metadata";
    public const string IncludeContent = "include.content";
    public const string ContentMaxSize = "content.max.size";

    // Defaults
    public const string DefaultMediaTypes = "all";
    public const int DefaultPollIntervalMs = 60000;
    public const int DefaultContentMaxSize = 10485760; // 10MB
}
