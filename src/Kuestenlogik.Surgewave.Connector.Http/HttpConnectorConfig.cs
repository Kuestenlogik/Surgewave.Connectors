namespace Kuestenlogik.Surgewave.Connector.Http;

/// <summary>
/// Configuration constants for HTTP connectors.
/// </summary>
public static class HttpConnectorConfig
{
    // Common HTTP settings
    public const string Url = "http.url";
    public const string Method = "http.method";
    public const string Headers = "http.headers";
    public const string ContentType = "content.type";
    public const string DefaultContentType = "application/json";

    // Source connector - polling
    public const string Topic = "topic";
    public const string PollIntervalMs = "poll.interval.ms";
    public const long DefaultPollIntervalMs = 5000;
    public const string ResponseMode = "response.mode";
    public const string ResponseModeRaw = "raw";
    public const string ResponseModeJsonArray = "json_array";

    // Source connector - webhook mode
    public const string SourceMode = "source.mode";
    public const string SourceModePoll = "poll";
    public const string SourceModeWebhook = "webhook";
    public const string WebhookPath = "webhook.path";
    public const string DefaultWebhookPath = "/webhooks/{name}";
    public const string WebhookSecret = "webhook.secret";
    public const string WebhookSignatureHeader = "webhook.signature.header";
    public const string DefaultSignatureHeader = "X-Signature-256";
    public const string WebhookSignatureAlgorithm = "webhook.signature.algorithm";
    public const string DefaultSignatureAlgorithm = "HMAC-SHA256";
    public const string WebhookValidateTimestamp = "webhook.validate.timestamp";
    public const string WebhookTimestampHeader = "webhook.timestamp.header";
    public const string DefaultTimestampHeader = "X-Timestamp";
    public const string WebhookTimestampToleranceMs = "webhook.timestamp.tolerance.ms";
    public const long DefaultTimestampToleranceMs = 300000; // 5 minutes

    // Sink connector
    public const string Topics = "topics";
    public const string BatchMode = "batch.mode";
    public const string BatchModeSingle = "single";
    public const string BatchModeArray = "array";
    public const string BatchSize = "batch.size";
    public const int DefaultBatchSize = 100;
    public const string RetryMax = "retry.max";
    public const int DefaultRetryMax = 3;
    public const string RetryBackoffMs = "retry.backoff.ms";
    public const long DefaultRetryBackoffMs = 1000;

    // Authentication
    public const string AuthType = "auth.type";
    public const string AuthTypeNone = "none";
    public const string AuthTypeBasic = "basic";
    public const string AuthTypeBearer = "bearer";
    public const string AuthTypeApiKey = "api_key";
    public const string AuthTypeHmac = "hmac";

    // Basic auth
    public const string AuthUsername = "auth.username";
    public const string AuthPassword = "auth.password";

    // Bearer auth
    public const string AuthToken = "auth.token";

    // API Key auth
    public const string AuthApiKey = "auth.api.key";
    public const string AuthApiKeyHeader = "auth.api.key.header";
    public const string DefaultApiKeyHeader = "X-API-Key";

    // HMAC auth (for signing outbound requests)
    public const string AuthHmacSecret = "auth.hmac.secret";
    public const string AuthHmacHeader = "auth.hmac.header";
    public const string AuthHmacAlgorithm = "auth.hmac.algorithm";

    // Offset tracking
    public const string OffsetLastPoll = "last_poll";
    public const string OffsetIndex = "index";
    public const string OffsetLastEventId = "last_event_id";

    // SSE (Server-Sent Events) - auto-detected via Content-Type: text/event-stream
    public const string SseContentType = "text/event-stream";
    public const string SseReconnectDelayMs = "sse.reconnect.delay.ms";
    public const long DefaultSseReconnectDelayMs = 3000;
}
