using System.Net.Http.Headers;
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

    private HttpClient? _httpClient;
    private string _pageId = string.Empty;
    private string _accessToken = string.Empty;
    private string _messageField = "message";
    private string? _linkField;
    private string _postType = "feed";

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _accessToken = config[FacebookConnectorConfig.AccessToken];
        _pageId = config[FacebookConnectorConfig.PageId];

        var apiVersion = config.TryGetValue(FacebookConnectorConfig.ApiVersion, out var v)
            ? v : FacebookConnectorConfig.DefaultApiVersion;

        _messageField = config.TryGetValue(FacebookConnectorConfig.MessageField, out var mf) ? mf : "message";
        _linkField = config.TryGetValue(FacebookConnectorConfig.LinkField, out var lf) ? lf : null;
        _postType = config.TryGetValue(FacebookConnectorConfig.PostType, out var pt) ? pt : "feed";

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri($"{FacebookConnectorConfig.BaseUrl}/{apiVersion}/")
        };
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_httpClient == null) return;

        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                var json = Encoding.UTF8.GetString(record.Value);
                var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json, JsonOptions);
                if (data == null) continue;

                var message = data.TryGetValue(_messageField, out var msgEl) ? msgEl.GetString() : json;

                var formData = new Dictionary<string, string>
                {
                    ["access_token"] = _accessToken,
                    ["message"] = message ?? ""
                };

                if (!string.IsNullOrEmpty(_linkField) && data.TryGetValue(_linkField, out var linkEl))
                {
                    var link = linkEl.GetString();
                    if (!string.IsNullOrEmpty(link))
                    {
                        formData["link"] = link;
                    }
                }

                using var content = new FormUrlEncodedContent(formData);
                var endpoint = _postType switch
                {
                    "photo" => $"{_pageId}/photos",
                    "video" => $"{_pageId}/videos",
                    _ => $"{_pageId}/feed"
                };

                using var response = await _httpClient.PostAsync(new Uri(endpoint, UriKind.Relative), content, cancellationToken);
            }
            catch (Exception)
            {
            }
        }
    }

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
