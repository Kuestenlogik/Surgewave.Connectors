using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Auth.OAuth2.Flows;
using Google.Apis.Auth.OAuth2.Responses;
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
    private GoogleAuthorizationCodeFlow? _authFlow;
    private string _topic = null!;
    private string? _albumId;
    private List<string> _albumNames = [];
    private string _mediaTypes = "all";
    private int _pollIntervalMs;
    private bool _includeMetadata;
    private bool _includeContent;
    private int _contentMaxSize;
    private bool _includeShared;
    private DateTime? _dateRangeStart;
    private DateTime? _dateRangeEnd;
    private DateTime _lastPoll = DateTime.MinValue;
    private readonly HashSet<string> _processedIds = [];
    private long _messageId;
    private HttpClient? _httpClient;

    public GooglePhotosSourceTask()
    {
    }

    /// <summary>
    /// Test seam: downloads media content through a caller-supplied HttpClient instead of
    /// creating one in Start.
    /// </summary>
    internal GooglePhotosSourceTask(HttpClient httpClient) => _httpClient = httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        ApplyConfig(config);
        _service = CreateService(config);
        _httpClient ??= new HttpClient();
    }

    /// <summary>
    /// Reads the task settings. Separated from credential and service construction so that
    /// filter and record building stay reachable without Google Photos credentials.
    /// </summary>
    internal void ApplyConfig(IDictionary<string, string> config)
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
        _includeShared = (config.TryGetValue(GooglePhotosConnectorConfig.IncludeShared, out var includeShared) ? includeShared : "false") == "true";

        if (config.TryGetValue(GooglePhotosConnectorConfig.DateRangeStart, out var dateStart) && !string.IsNullOrWhiteSpace(dateStart))
        {
            _dateRangeStart = DateTime.Parse(dateStart, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
        }

        if (config.TryGetValue(GooglePhotosConnectorConfig.DateRangeEnd, out var dateEnd) && !string.IsNullOrWhiteSpace(dateEnd))
        {
            _dateRangeEnd = DateTime.Parse(dateEnd, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
        }

        if (config.TryGetValue(GooglePhotosConnectorConfig.Albums, out var albums) && !string.IsNullOrWhiteSpace(albums))
        {
            _albumNames = albums.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        }
    }

    private PhotosLibraryService CreateService(IDictionary<string, string> config)
    {
        ICredential credential;

        if (config.TryGetValue(GooglePhotosConnectorConfig.CredentialsJson, out var json) && !string.IsNullOrWhiteSpace(json))
        {
            credential = GoogleCredential.FromJson(json).CreateScoped(PhotosLibraryService.Scope.PhotoslibraryReadonly);
        }
        else if (config.TryGetValue(GooglePhotosConnectorConfig.CredentialsFile, out var file) && !string.IsNullOrWhiteSpace(file))
        {
            credential = GoogleCredential.FromFile(file).CreateScoped(PhotosLibraryService.Scope.PhotoslibraryReadonly);
        }
        else
        {
            // OAuth2 flow: the refresh token is exchanged for access tokens via the client credentials
            var clientId = config[GooglePhotosConnectorConfig.ClientId];
            var clientSecret = config[GooglePhotosConnectorConfig.ClientSecret];
            var refreshToken = config[GooglePhotosConnectorConfig.RefreshToken];

            // owned by the task and disposed in Stop(): the credential keeps using
            // the flow for token refreshes after Start returns
            _authFlow = new GoogleAuthorizationCodeFlow(new GoogleAuthorizationCodeFlow.Initializer
            {
                ClientSecrets = new ClientSecrets { ClientId = clientId, ClientSecret = clientSecret },
                Scopes = [PhotosLibraryService.Scope.PhotoslibraryReadonly]
            });

            credential = new UserCredential(_authFlow, "user", new TokenResponse { RefreshToken = refreshToken });
        }

        return new PhotosLibraryService(new BaseClientService.Initializer
        {
            HttpClientInitializer = credential,
            ApplicationName = "Surgewave Google Photos Connector"
        });
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
        catch (Exception ex)
        {
            Context?.RaiseError?.Invoke(ex);
        }

        return records;
    }

    private async Task<IEnumerable<MediaItem>> FetchAlbumMediaAsync(string albumId, CancellationToken cancellationToken)
    {
        var items = new List<MediaItem>();
        string? pageToken = null;

        do
        {
            var request = _service!.MediaItems.Search(new SearchMediaItemsRequest
            {
                AlbumId = albumId,
                PageSize = 100,
                PageToken = pageToken
            });

            var response = await request.ExecuteAsync(cancellationToken);
            if (response.MediaItems != null)
            {
                items.AddRange(response.MediaItems);
            }

            pageToken = response.NextPageToken;
        } while (!string.IsNullOrEmpty(pageToken));

        return items;
    }

    private async Task<IEnumerable<MediaItem>> FetchAllMediaAsync(CancellationToken cancellationToken)
    {
        var items = new List<MediaItem>();
        string? pageToken = null;

        do
        {
            // Google Photos API doesn't have a simple list method - use search with optional filters
            var searchRequest = new SearchMediaItemsRequest
            {
                PageSize = 100,
                PageToken = pageToken,
                Filters = BuildDateFilters()
            };
            var request = _service!.MediaItems.Search(searchRequest);

            var response = await request.ExecuteAsync(cancellationToken);
            if (response.MediaItems != null)
            {
                items.AddRange(response.MediaItems);
            }

            pageToken = response.NextPageToken;
        } while (!string.IsNullOrEmpty(pageToken));

        return items;
    }

    internal Filters? BuildDateFilters()
    {
        if (_dateRangeStart == null && _dateRangeEnd == null)
        {
            return null;
        }

        // The API requires both ends of a range; open ends fall back to the epoch / today
        return new Filters
        {
            DateFilter = new DateFilter
            {
                Ranges =
                [
                    new DateRange
                    {
                        StartDate = ToApiDate(_dateRangeStart ?? DateTime.UnixEpoch),
                        EndDate = ToApiDate(_dateRangeEnd ?? DateTime.UtcNow)
                    }
                ]
            }
        };
    }

    private static Date ToApiDate(DateTime value) => new()
    {
        Year = value.Year,
        Month = value.Month,
        Day = value.Day
    };

    private async Task<Album?> FindAlbumByNameAsync(string name, CancellationToken cancellationToken)
    {
        var request = _service!.Albums.List();
        var response = await request.ExecuteAsync(cancellationToken);

        var album = response.Albums?.FirstOrDefault(a =>
            a.Title.Equals(name, StringComparison.OrdinalIgnoreCase));

        if (album != null || !_includeShared)
        {
            return album;
        }

        var sharedResponse = await _service.SharedAlbums.List().ExecuteAsync(cancellationToken);
        return sharedResponse.SharedAlbums?.FirstOrDefault(a =>
            name.Equals(a.Title, StringComparison.OrdinalIgnoreCase));
    }

    internal async Task<SourceRecord> CreateRecordAsync(MediaItem item, CancellationToken cancellationToken)
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
        _authFlow?.Dispose();
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
