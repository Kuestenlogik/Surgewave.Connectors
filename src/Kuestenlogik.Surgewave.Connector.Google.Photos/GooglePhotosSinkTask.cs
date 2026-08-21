using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
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
/// Task that uploads media items to Google Photos.
/// </summary>
[SuppressMessage("Reliability", "CA2213:Disposable fields should be disposed", Justification = "Disposed via Stop() called from Dispose()")]
public sealed class GooglePhotosSinkTask : SinkTask
{
    private PhotosLibraryService? _service;
    private HttpClient? _httpClient;
    private string? _albumId;
    private bool _createAlbum;
    private string? _albumTitle;
    private ICredential? _credential;
    private GoogleAuthorizationCodeFlow? _authFlow;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _albumId = config.TryGetValue(GooglePhotosConnectorConfig.UploadAlbumId, out var albumId) ? albumId : null;
        _createAlbum = (config.TryGetValue(GooglePhotosConnectorConfig.CreateAlbum, out var createAlbum) ? createAlbum : "false") == "true";
        _albumTitle = config.TryGetValue(GooglePhotosConnectorConfig.AlbumTitle, out var albumTitle) ? albumTitle : null;

        // Initialize Google Photos API client
        if (config.TryGetValue(GooglePhotosConnectorConfig.CredentialsJson, out var json) && !string.IsNullOrWhiteSpace(json))
        {
            _credential = GoogleCredential.FromJson(json).CreateScoped(
                PhotosLibraryService.Scope.Photoslibrary,
                PhotosLibraryService.Scope.PhotoslibraryAppendonly);
        }
        else if (config.TryGetValue(GooglePhotosConnectorConfig.CredentialsFile, out var file) && !string.IsNullOrWhiteSpace(file))
        {
            _credential = GoogleCredential.FromFile(file).CreateScoped(
                PhotosLibraryService.Scope.Photoslibrary,
                PhotosLibraryService.Scope.PhotoslibraryAppendonly);
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
                Scopes = [PhotosLibraryService.Scope.Photoslibrary, PhotosLibraryService.Scope.PhotoslibraryAppendonly]
            });

            _credential = new UserCredential(_authFlow, "user", new TokenResponse { RefreshToken = refreshToken });
        }

        _service = new PhotosLibraryService(new BaseClientService.Initializer
        {
            HttpClientInitializer = _credential,
            ApplicationName = "Surgewave Google Photos Connector"
        });

        _httpClient = new HttpClient();
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        // Ensure album exists if needed
        if (_createAlbum && !string.IsNullOrEmpty(_albumTitle) && string.IsNullOrEmpty(_albumId))
        {
            _albumId = await CreateOrFindAlbumAsync(_albumTitle, cancellationToken);
        }

        var uploadTokens = new List<(string token, string filename, string description)>();

        foreach (var record in records)
        {
            if (record.Value == null || record.Value.Length == 0)
                continue;

            // Get filename from headers or generate one
            var filename = "upload.jpg";
            if (record.Headers?.TryGetValue("google.photos.filename", out var filenameBytes) == true)
            {
                filename = Encoding.UTF8.GetString(filenameBytes);
            }
            else if (record.Headers?.TryGetValue("filename", out var fnBytes) == true)
            {
                filename = Encoding.UTF8.GetString(fnBytes);
            }

            // Get description
            var description = "";
            if (record.Headers?.TryGetValue("description", out var descBytes) == true)
            {
                description = Encoding.UTF8.GetString(descBytes);
            }

            // Upload bytes to get upload token
            var uploadToken = await UploadBytesAsync(record.Value, filename, cancellationToken);
            uploadTokens.Add((uploadToken, filename, description));
        }

        if (uploadTokens.Count > 0)
        {
            await CreateMediaItemsAsync(uploadTokens, cancellationToken);
        }
    }

    private async Task<string> UploadBytesAsync(byte[] content, string filename, CancellationToken cancellationToken)
    {
        try
        {
            var accessToken = await _credential!.GetAccessTokenForRequestAsync(cancellationToken: cancellationToken);

            using var request = new HttpRequestMessage(HttpMethod.Post, "https://photoslibrary.googleapis.com/v1/uploads")
            {
                Content = new ByteArrayContent(content)
            };

            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            request.Headers.Add("X-Goog-Upload-Content-Type", GetMimeType(filename));
            request.Headers.Add("X-Goog-Upload-Protocol", "raw");
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");

            using var response = await _httpClient!.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            Context?.RaiseError?.Invoke(ex);
            throw;
        }
    }

    private async Task CreateMediaItemsAsync(List<(string token, string filename, string description)> uploads, CancellationToken cancellationToken)
    {
        // Note: SimpleMediaItem uses UploadToken - filename is set during upload via headers
        var newMediaItems = uploads.Select(u => new NewMediaItem
        {
            SimpleMediaItem = new SimpleMediaItem
            {
                UploadToken = u.token
            },
            Description = string.IsNullOrEmpty(u.description) ? u.filename : u.description
        }).ToList();

        var request = new BatchCreateMediaItemsRequest
        {
            NewMediaItems = newMediaItems,
            AlbumId = _albumId
        };

        await _service!.MediaItems.BatchCreate(request).ExecuteAsync(cancellationToken);
    }

    private async Task<string?> CreateOrFindAlbumAsync(string title, CancellationToken cancellationToken)
    {
        // Try to find existing album
        var listRequest = _service!.Albums.List();
        var response = await listRequest.ExecuteAsync(cancellationToken);

        var existing = response.Albums?.FirstOrDefault(a =>
            a.Title.Equals(title, StringComparison.OrdinalIgnoreCase));

        if (existing != null)
            return existing.Id;

        // Create new album
        var createRequest = _service.Albums.Create(new CreateAlbumRequest
        {
            Album = new Album { Title = title }
        });

        var album = await createRequest.ExecuteAsync(cancellationToken);
        return album.Id;
    }

    private static string GetMimeType(string filename)
    {
        var ext = Path.GetExtension(filename).ToLowerInvariant();
        return ext switch
        {
            ".jpg" or ".jpeg" => "image/jpeg",
            ".png" => "image/png",
            ".gif" => "image/gif",
            ".webp" => "image/webp",
            ".heic" => "image/heic",
            ".mp4" => "video/mp4",
            ".mov" => "video/quicktime",
            ".avi" => "video/x-msvideo",
            _ => "application/octet-stream"
        };
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
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
