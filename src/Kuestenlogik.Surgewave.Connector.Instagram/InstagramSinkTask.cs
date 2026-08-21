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

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _accessToken = config[InstagramConnectorConfig.AccessToken];
        _accountId = config[InstagramConnectorConfig.BusinessAccountId];

        var apiVersion = config.TryGetValue(InstagramConnectorConfig.ApiVersion, out var v)
            ? v : InstagramConnectorConfig.DefaultApiVersion;

        _captionField = config.TryGetValue(InstagramConnectorConfig.CaptionField, out var cf) ? cf : "caption";
        _imageUrlField = config.TryGetValue(InstagramConnectorConfig.ImageUrlField, out var iuf) ? iuf : "image_url";

        var mediaType = config.TryGetValue(InstagramConnectorConfig.MediaType, out var mt) ? mt : "image";
        if (!string.Equals(mediaType, "image", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                $"'{InstagramConnectorConfig.MediaType}' value '{mediaType}' is not supported - only 'image' is implemented");
        }

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

            string caption;
            string? imageUrl;
            try
            {
                var json = Encoding.UTF8.GetString(record.Value);
                var data = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json, JsonOptions);
                if (data == null)
                {
                    RaisePoisonRecord(record, "record value is not a JSON object");
                    continue;
                }

                caption = (data.TryGetValue(_captionField, out var capEl) ? capEl.GetString() : "") ?? "";
                imageUrl = data.TryGetValue(_imageUrlField, out var imgEl) ? imgEl.GetString() : null;
            }
            catch (JsonException ex)
            {
                RaisePoisonRecord(record, $"record value is not valid JSON: {ex.Message}");
                continue;
            }

            if (string.IsNullOrEmpty(imageUrl))
            {
                RaisePoisonRecord(record, $"required field '{_imageUrlField}' is missing or empty");
                continue;
            }

            try
            {
                await PublishImageAsync(caption, imageUrl, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                var error = new InvalidOperationException(
                    $"Instagram publish failed for record at offset {record.Offset}: {ex.Message}", ex);
                Context?.RaiseError?.Invoke(error);
                throw error;
            }
        }
    }

    private async Task PublishImageAsync(string caption, string imageUrl, CancellationToken cancellationToken)
    {
        // Step 1: Create media container
        var containerParams = new Dictionary<string, string>
        {
            ["access_token"] = _accessToken,
            ["caption"] = caption,
            ["image_url"] = imageUrl
        };

        using var containerContent = new FormUrlEncodedContent(containerParams);
        using var containerResponse = await _httpClient!.PostAsync(
            new Uri($"{_accountId}/media", UriKind.Relative),
            containerContent,
            cancellationToken);

        if (!containerResponse.IsSuccessStatusCode)
        {
            throw new HttpRequestException(
                $"media container creation returned {(int)containerResponse.StatusCode} {containerResponse.StatusCode}");
        }

        var containerJson = await containerResponse.Content.ReadAsStringAsync(cancellationToken);
        var containerData = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(containerJson, JsonOptions);
        if (containerData == null || !containerData.TryGetValue("id", out var containerId))
        {
            throw new HttpRequestException("media container response did not contain an id");
        }

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

        if (!publishResponse.IsSuccessStatusCode)
        {
            throw new HttpRequestException(
                $"media publish returned {(int)publishResponse.StatusCode} {publishResponse.StatusCode}");
        }
    }

    private void RaisePoisonRecord(SinkRecord record, string reason)
    {
        Context?.RaiseError?.Invoke(new InvalidOperationException(
            $"Skipping Instagram record at offset {record.Offset}: {reason}"));
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
