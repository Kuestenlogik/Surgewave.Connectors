using System.Net;
using System.Text;
using Google.Apis.PhotosLibrary.v1.Data;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Google.Photos.Tests;

/// <summary>
/// Exercises the poll side without Google Photos credentials: the search filter a poll sends
/// and the record a media item turns into, including the optional content download.
/// </summary>
public class GooglePhotosSourceTaskTests
{
    [Fact]
    public void BuildDateFilters_WithoutAConfiguredRange_SendsNoFilter()
    {
        using var task = ConfiguredTask(_ => { });

        Assert.Null(task.BuildDateFilters());
    }

    [Fact]
    public void BuildDateFilters_WithBothEnds_CarriesTheConfiguredRange()
    {
        // google.photos.date.range.start / .end were declared in the ConfigDef and read nowhere,
        // so the filter never reached the API.
        using var task = ConfiguredTask(c =>
        {
            c[GooglePhotosConnectorConfig.DateRangeStart] = "2024-03-05";
            c[GooglePhotosConnectorConfig.DateRangeEnd] = "2024-04-06";
        });

        var filters = task.BuildDateFilters();

        Assert.NotNull(filters);
        var range = Assert.Single(filters!.DateFilter.Ranges);
        Assert.Equal(2024, range.StartDate.Year);
        Assert.Equal(3, range.StartDate.Month);
        Assert.Equal(5, range.StartDate.Day);
        Assert.Equal(2024, range.EndDate.Year);
        Assert.Equal(4, range.EndDate.Month);
        Assert.Equal(6, range.EndDate.Day);
    }

    [Fact]
    public void BuildDateFilters_WithOnlyAnEnd_OpensTheRangeAtTheEpoch()
    {
        using var task = ConfiguredTask(c => c[GooglePhotosConnectorConfig.DateRangeEnd] = "2024-04-06");

        var filters = task.BuildDateFilters();

        Assert.NotNull(filters);
        var range = Assert.Single(filters!.DateFilter.Ranges);
        Assert.Equal(1970, range.StartDate.Year);
        Assert.Equal(1, range.StartDate.Month);
        Assert.Equal(1, range.StartDate.Day);
        Assert.Equal(2024, range.EndDate.Year);
    }

    [Fact]
    public void BuildDateFilters_WithOnlyAStart_StillClosesTheRange()
    {
        using var task = ConfiguredTask(c => c[GooglePhotosConnectorConfig.DateRangeStart] = "2024-03-05");

        var filters = task.BuildDateFilters();

        Assert.NotNull(filters);
        var range = Assert.Single(filters!.DateFilter.Ranges);
        Assert.Equal(3, range.StartDate.Month);
        Assert.NotNull(range.EndDate);
    }

    [Fact]
    public async Task CreateRecordAsync_CarriesTheItemIdentityInKeyHeadersAndOffset()
    {
        using var task = ConfiguredTask(c => c[GooglePhotosConnectorConfig.IncludeMetadata] = "false");

        var record = await task.CreateRecordAsync(PhotoItem(), TestContext.Current.CancellationToken);

        Assert.Equal("google-photos", record.Topic);
        Assert.Equal("item-1", Encoding.UTF8.GetString(record.Key!));
        Assert.Equal("item-1", Encoding.UTF8.GetString(record.Headers!["google.photos.id"]));
        Assert.Equal("vacation.jpg", Encoding.UTF8.GetString(record.Headers!["google.photos.filename"]));
        Assert.Equal("image/jpeg", Encoding.UTF8.GetString(record.Headers!["google.photos.mime.type"]));
        Assert.Equal("google-photos", record.SourcePartition["source"]);
        Assert.Equal("item-1", record.SourceOffset["item_id"]);
        Assert.Equal(1L, record.SourceOffset["message_id"]);

        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"id\":\"item-1\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"mimeType\":\"image/jpeg\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"productUrl\":\"https://photos.example/item-1\"", payload, StringComparison.Ordinal);
        Assert.DoesNotContain("\"cameraMake\"", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateRecordAsync_WithMetadata_MarksAPhotoAndCarriesItsCameraFields()
    {
        using var task = ConfiguredTask(_ => { });

        var record = await task.CreateRecordAsync(PhotoItem(), TestContext.Current.CancellationToken);

        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"type\":\"photo\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"cameraMake\":\"Acme\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"cameraModel\":\"X100\"", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateRecordAsync_WithMetadata_MarksAVideo()
    {
        using var task = ConfiguredTask(_ => { });

        var item = PhotoItem();
        item.MediaMetadata = new MediaMetadata { Video = new Video { CameraMake = "Acme", Status = "READY" } };
        item.MimeType = "video/mp4";

        var record = await task.CreateRecordAsync(item, TestContext.Current.CancellationToken);

        var payload = Encoding.UTF8.GetString(record.Value);
        Assert.Contains("\"type\":\"video\"", payload, StringComparison.Ordinal);
        Assert.Contains("\"status\":\"READY\"", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateRecordAsync_WithoutADescription_FallsBackToTheItemIdAsFilename()
    {
        using var task = ConfiguredTask(_ => { });

        var item = PhotoItem();
        item.Description = null;

        var record = await task.CreateRecordAsync(item, TestContext.Current.CancellationToken);

        Assert.Equal("item-1", Encoding.UTF8.GetString(record.Headers!["google.photos.filename"]));
        Assert.Contains("\"filename\":\"item-1\"", Encoding.UTF8.GetString(record.Value), StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateRecordAsync_AssignsAscendingMessageIds()
    {
        using var task = ConfiguredTask(_ => { });

        var first = await task.CreateRecordAsync(PhotoItem(), TestContext.Current.CancellationToken);
        var second = await task.CreateRecordAsync(PhotoItem(), TestContext.Current.CancellationToken);

        Assert.Equal(1L, first.SourceOffset["message_id"]);
        Assert.Equal(2L, second.SourceOffset["message_id"]);
    }

    [Fact]
    public async Task CreateRecordAsync_WithContentEnabled_DownloadsTheOriginalAndUsesItAsTheValue()
    {
        var payload = new byte[] { 1, 2, 3, 4 };
        using var handler = new StubHttpHandler(_ => Bytes(HttpStatusCode.OK, payload));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = ConfiguredTask(c => c[GooglePhotosConnectorConfig.IncludeContent] = "true", http);

        var record = await task.CreateRecordAsync(PhotoItem(), TestContext.Current.CancellationToken);

        // "=d" is the Google Photos download parameter for the original bytes.
        Assert.Equal("https://photos.example/base=d", Assert.Single(handler.Requests));
        Assert.Equal(payload, record.Value);
    }

    [Fact]
    public async Task CreateRecordAsync_WithContentOverTheSizeLimit_KeepsTheMetadataPayload()
    {
        using var handler = new StubHttpHandler(_ => Bytes(HttpStatusCode.OK, [1, 2, 3, 4, 5]));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = ConfiguredTask(c =>
        {
            c[GooglePhotosConnectorConfig.IncludeContent] = "true";
            c[GooglePhotosConnectorConfig.ContentMaxSize] = "2";
        }, http);

        var record = await task.CreateRecordAsync(PhotoItem(), TestContext.Current.CancellationToken);

        Assert.Contains("\"id\":\"item-1\"", Encoding.UTF8.GetString(record.Value), StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateRecordAsync_WhenTheDownloadIsRejected_StillEmitsTheMetadataRecord()
    {
        using var handler = new StubHttpHandler(_ => new HttpResponseMessage(HttpStatusCode.Forbidden));
        using var http = new HttpClient(handler, disposeHandler: false);
        using var task = ConfiguredTask(c => c[GooglePhotosConnectorConfig.IncludeContent] = "true", http);

        var record = await task.CreateRecordAsync(PhotoItem(), TestContext.Current.CancellationToken);

        Assert.Contains("\"id\":\"item-1\"", Encoding.UTF8.GetString(record.Value), StringComparison.Ordinal);
        Assert.DoesNotContain("\"contentIncluded\"", Encoding.UTF8.GetString(record.Value), StringComparison.Ordinal);
    }

    private static MediaItem PhotoItem() => new()
    {
        Id = "item-1",
        Description = "vacation.jpg",
        MimeType = "image/jpeg",
        BaseUrl = "https://photos.example/base",
        ProductUrl = "https://photos.example/item-1",
        MediaMetadata = new MediaMetadata
        {
            Photo = new Photo { CameraMake = "Acme", CameraModel = "X100" }
        }
    };

    private static HttpResponseMessage Bytes(HttpStatusCode status, byte[] content) =>
        new(status) { Content = new ByteArrayContent(content) };

    private static GooglePhotosSourceTask ConfiguredTask(
        Action<Dictionary<string, string>> configure,
        HttpClient? httpClient = null)
    {
        var config = SourceConfig();
        configure(config);

        var task = httpClient == null ? new GooglePhotosSourceTask() : new GooglePhotosSourceTask(httpClient);
        task.Initialize(new TaskContext { RaiseError = _ => { } });
        task.ApplyConfig(config);
        return task;
    }

    private static Dictionary<string, string> SourceConfig() => new()
    {
        [GooglePhotosConnectorConfig.Topic] = "google-photos",
        [GooglePhotosConnectorConfig.PollIntervalMs] = "0"
    };

    /// <summary>Answers every download from a canned responder and records the URLs it saw.</summary>
    private sealed class StubHttpHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public List<string> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests.Add(request.RequestUri!.ToString());
            return Task.FromResult(respond(request));
        }
    }
}
