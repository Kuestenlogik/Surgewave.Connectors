using System.Globalization;
using System.Net.Http.Headers;
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

    private const string MessageTypeText = "text";
    private const string MessageTypeQuickReplies = "quick_replies";

    private HttpClient? _httpClient;
    private string? _defaultRecipientId;
    private string _recipientIdField = "recipient_id";
    private string _messageTextField = "text";
    private string _messageType = MessageTypeText;
    private string _quickRepliesField = "quick_replies";

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var accessToken = config[MessengerConnectorConfig.PageAccessToken];

        var apiVersion = config.TryGetValue(MessengerConnectorConfig.ApiVersion, out var v)
            ? v : MessengerConnectorConfig.DefaultApiVersion;

        _defaultRecipientId = config.TryGetValue(MessengerConnectorConfig.DefaultRecipientId, out var dr) ? dr : null;
        _recipientIdField = config.TryGetValue(MessengerConnectorConfig.RecipientIdField, out var rif) ? rif : "recipient_id";
        _messageTextField = config.TryGetValue(MessengerConnectorConfig.MessageTextField, out var mtf) ? mtf : "text";
        _messageType = config.TryGetValue(MessengerConnectorConfig.MessageType, out var mt) ? mt : MessageTypeText;
        _quickRepliesField = config.TryGetValue(MessengerConnectorConfig.QuickRepliesField, out var qrf) ? qrf : "quick_replies";

        if (_messageType is not (MessageTypeText or MessageTypeQuickReplies))
        {
            throw new ArgumentException(
                $"Unsupported '{MessengerConnectorConfig.MessageType}' value '{_messageType}'. Supported: {MessageTypeText}, {MessageTypeQuickReplies}.",
                nameof(config));
        }

        _httpClient = new HttpClient
        {
            BaseAddress = new Uri($"{MessengerConnectorConfig.BaseUrl}/{apiVersion}/")
        };

        // Send the page token as a bearer header - a query string ends up in proxy and
        // diagnostics logs.
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
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

            var recipientId = _defaultRecipientId;
            if (data.TryGetValue(_recipientIdField, out var ridEl) && ridEl.ValueKind == JsonValueKind.String)
            {
                recipientId = ridEl.GetString();
            }

            if (string.IsNullOrEmpty(recipientId))
            {
                Context?.RaiseError?.Invoke(new InvalidOperationException(
                    Describe(record, $"has no '{_recipientIdField}' recipient and no configured default")));
                continue;
            }

            var text = data.TryGetValue(_messageTextField, out var textEl)
                ? (textEl.ValueKind == JsonValueKind.String ? textEl.GetString() : textEl.ToString())
                : json;

            var message = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["text"] = text ?? string.Empty
            };

            if (_messageType == MessageTypeQuickReplies)
            {
                var quickReplies = BuildQuickReplies(data);
                if (quickReplies.Count == 0)
                {
                    Context?.RaiseError?.Invoke(new InvalidOperationException(
                        Describe(record, $"has no usable '{_quickRepliesField}' array required for a '{MessageTypeQuickReplies}' message")));
                    continue;
                }

                message["quick_replies"] = quickReplies;
            }

            var payload = new Dictionary<string, object?>(StringComparer.Ordinal)
            {
                ["recipient"] = new Dictionary<string, object?>(StringComparer.Ordinal) { ["id"] = recipientId },
                ["message"] = message
            };

            using var content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json");
            using var response = await _httpClient.PostAsync(
                new Uri("me/messages", UriKind.Relative),
                content,
                cancellationToken);

            // An undelivered message must fail the batch so the worker can retry or DLQ it -
            // the records are acked once PutAsync returns.
            if (!response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(cancellationToken);
                var error = new HttpRequestException(string.Create(CultureInfo.InvariantCulture,
                    $"Messenger Send API rejected the message with status {(int)response.StatusCode} {response.ReasonPhrase}: {body}"));
                Context?.RaiseError?.Invoke(error);
                throw error;
            }
        }
    }

    private List<Dictionary<string, object?>> BuildQuickReplies(Dictionary<string, JsonElement> data)
    {
        var replies = new List<Dictionary<string, object?>>();

        if (!data.TryGetValue(_quickRepliesField, out var element) || element.ValueKind != JsonValueKind.Array)
        {
            return replies;
        }

        foreach (var item in element.EnumerateArray())
        {
            if (item.ValueKind == JsonValueKind.String)
            {
                var title = item.GetString();
                if (!string.IsNullOrEmpty(title))
                {
                    replies.Add(CreateQuickReply("text", title, title));
                }
            }
            else if (item.ValueKind == JsonValueKind.Object)
            {
                var title = ReadString(item, "title");
                if (string.IsNullOrEmpty(title)) continue;

                replies.Add(CreateQuickReply(ReadString(item, "content_type") ?? "text", title, ReadString(item, "payload") ?? title));
            }
        }

        return replies;
    }

    private static string? ReadString(JsonElement element, string propertyName) =>
        element.TryGetProperty(propertyName, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    private static Dictionary<string, object?> CreateQuickReply(string contentType, string title, string payload) =>
        new(StringComparer.Ordinal)
        {
            ["content_type"] = contentType,
            ["title"] = title,
            ["payload"] = payload
        };

    private static string Describe(SinkRecord record, string problem) => string.Create(
        CultureInfo.InvariantCulture,
        $"Messenger sink skipped record {record.Topic}-{record.Partition}@{record.Offset}: it {problem}");

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
