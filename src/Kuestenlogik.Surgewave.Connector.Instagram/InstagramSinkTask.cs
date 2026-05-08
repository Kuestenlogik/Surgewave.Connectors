using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Instagram;

/// <summary>
/// Sink task that publishes media to Instagram via Graph API.
/// </summary>
#pragma warning disable CA2213
public sealed class InstagramSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _accessToken = string.Empty;
    private string _accountId = string.Empty;
    private string _captionField = "caption";
    private string _imageUrlField = "image_url";
    private string _mediaType = "image";

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _accessToken = config[InstagramConnectorConfig.AccessToken];
        _accountId = config[InstagramConnectorConfig.BusinessAccountId];

        var apiVersion = config.TryGetValue(InstagramConnectorConfig.ApiVersion, out var v)
            ? v : InstagramConnectorConfig.DefaultApiVersion;

        _captionField = config.TryGetValue(InstagramConnectorConfig.CaptionField, out var cf) ? cf : "caption";
        _imageUrlField = config.TryGetValue(InstagramConnectorConfig.ImageUrlField, out var iuf) ? iuf : "image_url";
        _mediaType = config.TryGetValue(InstagramConnectorConfig.MediaType, out var mt) ? mt : "image";

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri($"{InstagramConnectorConfig.BaseUrl}/{apiVersion}/")
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

                var caption = data.TryGetValue(_captionField, out var capEl) ? capEl.GetString() : "";
                var imageUrl = data.TryGetValue(_imageUrlField, out var imgEl) ? imgEl.GetString() : null;

                if (string.IsNullOrEmpty(imageUrl)) continue;

                // Step 1: Create media container
                var containerParams = new Dictionary<string, string>
                {
                    ["access_token"] = _accessToken,
                    ["caption"] = caption ?? "",
                    ["image_url"] = imageUrl
                };

                using var containerContent = new FormUrlEncodedContent(containerParams);
                using var containerResponse = await _httpClient.PostAsync(
                    new Uri($"{_accountId}/media", UriKind.Relative),
                    containerContent,
                    cancellationToken);

                if (!containerResponse.IsSuccessStatusCode) continue;

                var containerJson = await containerResponse.Content.ReadAsStringAsync(cancellationToken);
                var containerData = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(containerJson, JsonOptions);
                if (containerData == null || !containerData.TryGetValue("id", out var containerId)) continue;

                // Step 2: Publish media
                var publishParams = new Dictionary<string, string>
                {
                    ["access_token"] = _accessToken,
                    ["creation_id"] = containerId.GetString() ?? ""
                };

                using var publishContent = new FormUrlEncodedContent(publishParams);
                using var publishResponse = await _httpClient.PostAsync(
                    new Uri($"{_accountId}/media_publish", UriKind.Relative),
                    publishContent,
                    cancellationToken);
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
