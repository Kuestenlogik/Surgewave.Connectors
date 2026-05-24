using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Apis.PhotosLibrary.v1;
using Google.Apis.PhotosLibrary.v1.Data;
using Google.Apis.Services;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Google.Photos;

/// <summary>
/// Task that polls Google Photos for new media items.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class GooglePhotosSourceTask : SourceTask
{
    private PhotosLibraryService? _service;
    private string _topic = null!;
    private string? _albumId;
    private List<string> _albumNames = [];
    private string _mediaTypes = "all";
    private int _pollIntervalMs;
    private bool _includeMetadata;
    private bool _includeContent;
    private int _contentMaxSize;
    private DateTime _lastPoll = DateTime.MinValue;
    private readonly HashSet<string> _processedIds = [];
    private long _messageId;
    private HttpClient? _httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _topic = config[GooglePhotosConnectorConfig.Topic];
        _albumId = config.TryGetValue(GooglePhotosConnectorConfig.AlbumId, out var albumId) ? albumId : null;
        _mediaTypes = config.TryGetValue(GooglePhotosConnectorConfig.MediaTypes, out var mediaTypes) ? mediaTypes : "all";
        _pollIntervalMs = int.Parse(config.TryGetValue(GooglePhotosConnectorConfig.PollIntervalMs, out var pollInterval)
            ? pollInterval : GooglePhotosConnectorConfig.DefaultPollIntervalMs.ToString());
        _includeMetadata = (config.TryGetValue(GooglePhotosConnectorConfig.IncludeMetadata, out var includeMetadata) ? includeMetadata : "true") == "true";
        _includeContent = (config.TryGetValue(GooglePhotosConnectorConfig.IncludeContent, out var includeContent) ? includeContent : "false") == "true";
        _contentMaxSize = int.Parse(config.TryGetValue(GooglePhotosConnectorConfig.ContentMaxSize, out var contentMaxSize)
            ? contentMaxSize : GooglePhotosConnectorConfig.DefaultContentMaxSize.ToString());

        if (config.TryGetValue(GooglePhotosConnectorConfig.Albums, out var albums) && !string.IsNullOrWhiteSpace(albums))
        {
            _albumNames = albums.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        }

        // Initialize Google Photos API client
        GoogleCredential credential;

        if (config.TryGetValue(GooglePhotosConnectorConfig.CredentialsJson, out var json) && !string.IsNullOrWhiteSpace(json))
        {
            credential = GoogleCredential.FromJson(json);
        }
        else if (config.TryGetValue(GooglePhotosConnectorConfig.CredentialsFile, out var file) && !string.IsNullOrWhiteSpace(file))
        {
            credential = GoogleCredential.FromFile(file);
        }
        else
        {
            // OAuth2 flow
            var clientId = config[GooglePhotosConnectorConfig.ClientId];
            var clientSecret = config[GooglePhotosConnectorConfig.ClientSecret];
            var refreshToken = config[GooglePhotosConnectorConfig.RefreshToken];

            credential = GoogleCredential.FromAccessToken(refreshToken);
        }

        credential = credential.CreateScoped(PhotosLibraryService.Scope.PhotoslibraryReadonly);

        _service = new PhotosLibraryService(new BaseClientService.Initializer
        {
            HttpClientInitializer = credential,
            ApplicationName = "Surgewave Google Photos Connector"
        });

        _httpClient = new HttpClient();
    }

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        // Check poll interval
        if ((DateTime.UtcNow - _lastPoll).TotalMilliseconds < _pollIntervalMs)
        {
            return [];
        }

        _lastPoll = DateTime.UtcNow;
        var records = new List<SourceRecord>();

        try
        {
            IEnumerable<MediaItem> mediaItems;

            if (!string.IsNullOrEmpty(_albumId))
            {
                // Fetch from specific album
                mediaItems = await FetchAlbumMediaAsync(_albumId, cancellationToken);
            }
            else if (_albumNames.Count > 0)
            {
                // Fetch from named albums
                var allItems = new List<MediaItem>();
                foreach (var albumName in _albumNames)
                {
                    var album = await FindAlbumByNameAsync(albumName, cancellationToken);
                    if (album != null)
                    {
                        var items = await FetchAlbumMediaAsync(album.Id, cancellationToken);
                        allItems.AddRange(items);
                    }
                }
                mediaItems = allItems;
            }
            else
            {
                // Fetch all media
                mediaItems = await FetchAllMediaAsync(cancellationToken);
            }

            foreach (var item in mediaItems)
            {
                if (_processedIds.Contains(item.Id))
                    continue;

                // Filter by media type
                if (_mediaTypes != "all")
                {
                    var isPhoto = item.MediaMetadata?.Photo != null;
                    var isVideo = item.MediaMetadata?.Video != null;

                    if (_mediaTypes == "photo" && !isPhoto) continue;
                    if (_mediaTypes == "video" && !isVideo) continue;
                }

                var record = await CreateRecordAsync(item, cancellationToken);
                records.Add(record);
                _processedIds.Add(item.Id);
            }
        }
        catch (Exception)
        {
            // Log and continue
        }

        return records;
    }

    private async Task<IEnumerable<MediaItem>> FetchAlbumMediaAsync(string albumId, CancellationToken cancellationToken)
    {
        var items = new List<MediaItem>();
        var request = _service!.MediaItems.Search(new SearchMediaItemsRequest { AlbumId = albumId });

        var response = await request.ExecuteAsync(cancellationToken);
        if (response.MediaItems != null)
        {
            items.AddRange(response.MediaItems);
        }

        return items;
    }

    private async Task<IEnumerable<MediaItem>> FetchAllMediaAsync(CancellationToken cancellationToken)
    {
        var items = new List<MediaItem>();
        // Google Photos API doesn't have a simple list method - use search without filters
        var searchRequest = new SearchMediaItemsRequest { PageSize = 100 };
        var request = _service!.MediaItems.Search(searchRequest);

        var response = await request.ExecuteAsync(cancellationToken);
        if (response.MediaItems != null)
        {
            items.AddRange(response.MediaItems);
        }

        return items;
    }

    private async Task<Album?> FindAlbumByNameAsync(string name, CancellationToken cancellationToken)
    {
        var request = _service!.Albums.List();
        var response = await request.ExecuteAsync(cancellationToken);

        return response.Albums?.FirstOrDefault(a =>
            a.Title.Equals(name, StringComparison.OrdinalIgnoreCase));
    }

    private async Task<SourceRecord> CreateRecordAsync(MediaItem item, CancellationToken cancellationToken)
    {
        // Note: The Google Photos API filename property may vary by SDK version
        var filename = item.Description ?? item.Id; // Fallback if filename not available
        var payload = new Dictionary<string, object?>
        {
            ["id"] = item.Id,
            ["filename"] = filename,
            ["mimeType"] = item.MimeType,
            ["baseUrl"] = item.BaseUrl,
            ["productUrl"] = item.ProductUrl
        };

        if (_includeMetadata && item.MediaMetadata != null)
        {
            payload["creationTime"] = item.MediaMetadata.CreationTime;
            payload["width"] = item.MediaMetadata.Width;
            payload["height"] = item.MediaMetadata.Height;

            if (item.MediaMetadata.Photo != null)
            {
                payload["type"] = "photo";
                payload["cameraMake"] = item.MediaMetadata.Photo.CameraMake;
                payload["cameraModel"] = item.MediaMetadata.Photo.CameraModel;
                payload["focalLength"] = item.MediaMetadata.Photo.FocalLength;
                payload["apertureFNumber"] = item.MediaMetadata.Photo.ApertureFNumber;
                payload["isoEquivalent"] = item.MediaMetadata.Photo.IsoEquivalent;
            }
            else if (item.MediaMetadata.Video != null)
            {
                payload["type"] = "video";
                payload["cameraMake"] = item.MediaMetadata.Video.CameraMake;
                payload["cameraModel"] = item.MediaMetadata.Video.CameraModel;
                payload["fps"] = item.MediaMetadata.Video.Fps;
                payload["status"] = item.MediaMetadata.Video.Status;
            }
        }

        byte[]? content = null;
        if (_includeContent && !string.IsNullOrEmpty(item.BaseUrl))
        {
            try
            {
                var downloadUrl = new Uri(item.BaseUrl + "=d"); // Download parameter
                using var response = await _httpClient!.GetAsync(downloadUrl, cancellationToken);
                if (response.IsSuccessStatusCode)
                {
                    var bytes = await response.Content.ReadAsByteArrayAsync(cancellationToken);
                    if (bytes.Length <= _contentMaxSize)
                    {
                        content = bytes;
                        payload["contentIncluded"] = true;
                        payload["contentSize"] = bytes.Length;
                    }
                }
            }
            catch
            {
                // Content download failed, continue without it
            }
        }

        var headers = new Dictionary<string, byte[]>
        {
            ["google.photos.id"] = Encoding.UTF8.GetBytes(item.Id),
            ["google.photos.filename"] = Encoding.UTF8.GetBytes(filename ?? ""),
            ["google.photos.mime.type"] = Encoding.UTF8.GetBytes(item.MimeType ?? "")
        };

        return new SourceRecord
        {
            SourcePartition = new Dictionary<string, object> { ["source"] = "google-photos" },
            SourceOffset = new Dictionary<string, object>
            {
                ["item_id"] = item.Id,
                ["message_id"] = Interlocked.Increment(ref _messageId)
            },
            Topic = _topic,
            Key = Encoding.UTF8.GetBytes(item.Id),
            Value = content ?? JsonSerializer.SerializeToUtf8Bytes(payload),
            Headers = headers
        };
    }

    public override void Stop()
    {
        _service?.Dispose();
        _httpClient?.Dispose();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
