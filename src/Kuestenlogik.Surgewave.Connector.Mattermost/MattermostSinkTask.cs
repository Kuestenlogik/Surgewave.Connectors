using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Mattermost;

/// <summary>
/// Sink task that posts messages to Mattermost channels via REST API.
/// </summary>
#pragma warning disable CA2213
public sealed class MattermostSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _channelId = string.Empty;
    private string _messageField = MattermostConnectorConfig.DefaultMessageField;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var serverUrl = (config.TryGetValue(MattermostConnectorConfig.ServerUrl, out var su) ? su : MattermostConnectorConfig.DefaultServerUrl).TrimEnd('/');
        var accessToken = config[MattermostConnectorConfig.AccessToken];
        _channelId = config[MattermostConnectorConfig.ChannelId];
        _messageField = config.TryGetValue(MattermostConnectorConfig.MessageField, out var mf) ? mf : MattermostConnectorConfig.DefaultMessageField;

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(serverUrl),
            DefaultRequestHeaders =
            {
                Authorization = new AuthenticationHeaderValue("Bearer", accessToken)
            }
        };
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_httpClient == null)
            return;

        foreach (var record in records)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var message = ExtractMessage(record);
            if (string.IsNullOrWhiteSpace(message))
                continue;

            var payload = new { channel_id = _channelId, message };
            using var content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json");

            var response = await _httpClient.PostAsync(new Uri("/api/v4/posts", UriKind.Relative), content, cancellationToken);
            response.EnsureSuccessStatusCode();
        }
    }

    private string? ExtractMessage(SinkRecord record)
    {
        if (record.Value == null || record.Value.Length == 0)
            return null;

        try
        {
            var json = Encoding.UTF8.GetString(record.Value);
            using var doc = JsonDocument.Parse(json);

            if (doc.RootElement.TryGetProperty(_messageField, out var messageElement))
            {
                return messageElement.GetString();
            }

            // If not JSON or field not found, use raw value as message
            return json;
        }
        catch
        {
            return Encoding.UTF8.GetString(record.Value);
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
        if (disposing)
        {
            Stop();
        }
        base.Dispose(disposing);
    }
}
#pragma warning restore CA2213
