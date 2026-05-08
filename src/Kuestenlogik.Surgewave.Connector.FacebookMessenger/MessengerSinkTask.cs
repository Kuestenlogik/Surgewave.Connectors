using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.FacebookMessenger;

/// <summary>
/// Sink task that sends messages via Facebook Messenger Platform API.
/// </summary>
#pragma warning disable CA2213
public sealed class MessengerSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _accessToken = string.Empty;
    private string? _defaultRecipientId;
    private string _recipientIdField = "recipient_id";
    private string _messageTextField = "text";
    private string _messageType = "text";

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _accessToken = config[MessengerConnectorConfig.PageAccessToken];

        var apiVersion = config.TryGetValue(MessengerConnectorConfig.ApiVersion, out var v)
            ? v : MessengerConnectorConfig.DefaultApiVersion;

        _defaultRecipientId = config.TryGetValue(MessengerConnectorConfig.DefaultRecipientId, out var dr) ? dr : null;
        _recipientIdField = config.TryGetValue(MessengerConnectorConfig.RecipientIdField, out var rif) ? rif : "recipient_id";
        _messageTextField = config.TryGetValue(MessengerConnectorConfig.MessageTextField, out var mtf) ? mtf : "text";
        _messageType = config.TryGetValue(MessengerConnectorConfig.MessageType, out var mt) ? mt : "text";

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri($"{MessengerConnectorConfig.BaseUrl}/{apiVersion}/")
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

                var recipientId = _defaultRecipientId;
                if (data.TryGetValue(_recipientIdField, out var ridEl))
                {
                    recipientId = ridEl.GetString();
                }

                if (string.IsNullOrEmpty(recipientId)) continue;

                var text = data.TryGetValue(_messageTextField, out var textEl) ? textEl.GetString() : json;

                object payload = new
                {
                    recipient = new { id = recipientId },
                    message = new { text }
                };

                using var content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json");
                using var response = await _httpClient.PostAsync(
                    new Uri($"me/messages?access_token={_accessToken}", UriKind.Relative),
                    content,
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
