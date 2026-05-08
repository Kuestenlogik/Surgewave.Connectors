using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.WhatsApp;

/// <summary>
/// Sink task that sends messages via WhatsApp Business Cloud API.
/// </summary>
#pragma warning disable CA2213
public sealed class WhatsAppSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false
    };

    private HttpClient? _httpClient;
    private string _phoneNumberId = string.Empty;
    private string? _defaultRecipient;
    private string _recipientField = "to";
    private string _messageField = "text";
    private string _messageType = "text";

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var accessToken = config[WhatsAppConnectorConfig.AccessToken];
        _phoneNumberId = config[WhatsAppConnectorConfig.PhoneNumberId];

        var apiVersion = config.TryGetValue(WhatsAppConnectorConfig.ApiVersion, out var v)
            ? v : WhatsAppConnectorConfig.DefaultApiVersion;

        _defaultRecipient = config.TryGetValue(WhatsAppConnectorConfig.DefaultRecipient, out var dr) ? dr : null;
        _recipientField = config.TryGetValue(WhatsAppConnectorConfig.RecipientField, out var rf) ? rf : "to";
        _messageField = config.TryGetValue(WhatsAppConnectorConfig.MessageField, out var mf) ? mf : "text";
        _messageType = config.TryGetValue(WhatsAppConnectorConfig.MessageType, out var mt) ? mt : "text";

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri($"{WhatsAppConnectorConfig.BaseUrl}/{apiVersion}/")
        };
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
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

                var recipient = _defaultRecipient;
                if (data.TryGetValue(_recipientField, out var recipientEl))
                {
                    recipient = recipientEl.GetString();
                }

                if (string.IsNullOrEmpty(recipient)) continue;

                var text = data.TryGetValue(_messageField, out var textEl) ? textEl.GetString() : json;

                object payload = _messageType switch
                {
                    "template" => new
                    {
                        messaging_product = "whatsapp",
                        to = recipient,
                        type = "template",
                        template = new
                        {
                            name = data.TryGetValue("template_name", out var tn) ? tn.GetString() : "hello_world",
                            language = new { code = data.TryGetValue("template_language", out var tl) ? tl.GetString() : "en_US" }
                        }
                    },
                    _ => new
                    {
                        messaging_product = "whatsapp",
                        to = recipient,
                        type = "text",
                        text = new { body = text }
                    }
                };

                using var content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json");
                using var response = await _httpClient.PostAsync(new Uri($"{_phoneNumberId}/messages", UriKind.Relative), content, cancellationToken);
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
