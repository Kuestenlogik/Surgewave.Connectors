using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Facebook;

/// <summary>
/// Sink task that posts to Facebook pages via Graph API.
/// </summary>
#pragma warning disable CA2213
public sealed class FacebookSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    private readonly HttpMessageHandler? _handler;
    private HttpClient? _httpClient;
    private string _pageId = string.Empty;
    private string _accessToken = string.Empty;
    private string _messageField = "message";
    private string? _linkField;
    private string _postType = PostTypeFeed;
    private string _imageUrlField = "image_url";

    private const string PostTypeFeed = "feed";
    private const string PostTypePhoto = "photo";
    private const string PostTypeVideo = "video";

    public override string Version => "1.0.0";

    public FacebookSinkTask()
    {
    }

    /// <summary>
    /// Builds the Graph API client on a caller-supplied transport instead of a socket of its own.
    /// </summary>
    internal FacebookSinkTask(HttpMessageHandler handler) => _handler = handler;

    public override void Start(IDictionary<string, string> config)
    {
        _accessToken = config[FacebookConnectorConfig.AccessToken];
        _pageId = config[FacebookConnectorConfig.PageId];

        var apiVersion = config.TryGetValue(FacebookConnectorConfig.ApiVersion, out var v)
            ? v : FacebookConnectorConfig.DefaultApiVersion;

        _messageField = config.TryGetValue(FacebookConnectorConfig.MessageField, out var mf) ? mf : "message";
        _linkField = config.TryGetValue(FacebookConnectorConfig.LinkField, out var lf) ? lf : null;
        _postType = config.TryGetValue(FacebookConnectorConfig.PostType, out var pt) ? pt : PostTypeFeed;
        _imageUrlField = config.TryGetValue(FacebookConnectorConfig.ImageUrlField, out var iuf) ? iuf : "image_url";

        if (_postType is not (PostTypeFeed or PostTypePhoto or PostTypeVideo))
        {
            throw new ArgumentException(
                $"Unsupported '{FacebookConnectorConfig.PostType}' value '{_postType}'. Supported: {PostTypeFeed}, {PostTypePhoto}, {PostTypeVideo}.",
                nameof(config));
        }

        _httpClient = _handler == null ? new HttpClient() : new HttpClient(_handler, disposeHandler: false);
        _httpClient.BaseAddress = new Uri($"{FacebookConnectorConfig.BaseUrl}/{apiVersion}/");
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_httpClient == null) return;

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            var json = Encoding.UTF8.GetString(record.Value);

            Dictionary<string, JsonElement>? data;
            try
            {
                data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json, JsonOptions);
            }
            catch (JsonException ex)
            {
                // Poison record: skip it, but keep it visible instead of acking silently
                Context?.RaiseError?.Invoke(new InvalidOperationException(
                    Describe(record, "is not a JSON object"), ex));
                continue;
            }

            if (data == null) continue;

            var message = data.TryGetValue(_messageField, out var msgEl)
                ? (msgEl.ValueKind == JsonValueKind.String ? msgEl.GetString() : msgEl.ToString())
                : json;

            var formData = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["access_token"] = _accessToken
            };

            string endpoint;
            if (_postType is PostTypePhoto or PostTypeVideo)
            {
                // The Graph API rejects a /photos or /videos post without a media source,
                // so a record without one is a poison record, not a deliverable post.
                var mediaUrl = data.TryGetValue(_imageUrlField, out var mediaEl) && mediaEl.ValueKind == JsonValueKind.String
                    ? mediaEl.GetString()
                    : null;
                if (string.IsNullOrEmpty(mediaUrl))
                {
                    Context?.RaiseError?.Invoke(new InvalidOperationException(
                        Describe(record, $"has no '{_imageUrlField}' media URL required for a '{_postType}' post")));
                    continue;
                }

                if (_postType == PostTypePhoto)
                {
                    endpoint = $"{_pageId}/photos";
                    formData["url"] = mediaUrl;
                    formData["caption"] = message ?? string.Empty;
                }
                else
                {
                    endpoint = $"{_pageId}/videos";
                    formData["file_url"] = mediaUrl;
                    formData["description"] = message ?? string.Empty;
                }
            }
            else
            {
                endpoint = $"{_pageId}/feed";
                formData["message"] = message ?? string.Empty;

                if (!string.IsNullOrEmpty(_linkField) && data.TryGetValue(_linkField, out var linkEl)
                    && linkEl.ValueKind == JsonValueKind.String)
                {
                    var link = linkEl.GetString();
                    if (!string.IsNullOrEmpty(link))
                    {
                        formData["link"] = link;
                    }
                }
            }

            using var content = new FormUrlEncodedContent(formData);
            using var response = await _httpClient.PostAsync(new Uri(endpoint, UriKind.Relative), content, cancellationToken);

            // A rejected post (bad token, rate limit, API error) must fail the batch so the
            // worker can retry or DLQ it - the records are acked once PutAsync returns.
            if (!response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(cancellationToken);
                var error = new HttpRequestException(string.Create(CultureInfo.InvariantCulture,
                    $"Facebook Graph API rejected the post to /{endpoint} with status {(int)response.StatusCode} {response.ReasonPhrase}: {body}"));
                Context?.RaiseError?.Invoke(error);
                throw error;
            }
        }
    }

    private static string Describe(SinkRecord record, string problem) => string.Create(
        CultureInfo.InvariantCulture,
        $"Facebook sink skipped record {record.Topic}-{record.Partition}@{record.Offset}: it {problem}");

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
        _httpClient?.Dispose();
        _httpClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Stop();
        base.Dispose(disposing);
    }
}
#pragma warning restore CA2213
