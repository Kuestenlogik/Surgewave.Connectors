using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Http;

/// <summary>
/// A sink connector that sends records to an HTTP endpoint.
/// Supports batching, various content types, and multiple authentication methods.
/// </summary>
public sealed class HttpSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(HttpSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Required settings
        .Define(HttpConnectorConfig.Url, ConfigType.String, Importance.High,
            "HTTP URL to send records to")
        .Define(HttpConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Topics to consume from (comma-separated)", EditorHint.Topic)

        // HTTP settings
        .Define(HttpConnectorConfig.Method, ConfigType.String, "POST", Importance.Medium,
            "HTTP method (POST or PUT)")
        .Define(HttpConnectorConfig.ContentType, ConfigType.String, HttpConnectorConfig.DefaultContentType, Importance.Medium,
            "Content-Type header value")
        .Define(HttpConnectorConfig.Headers, ConfigType.String, "", Importance.Low,
            "Additional HTTP headers as key=value pairs separated by semicolons", EditorHint.Multiline)

        // Batching
        .Define(HttpConnectorConfig.BatchMode, ConfigType.String, HttpConnectorConfig.BatchModeSingle, Importance.Medium,
            "Batch mode: 'single' (one request per record) or 'array' (batch records as JSON array)", EditorHint.Select, options: ["single", "array"])
        .Define(HttpConnectorConfig.BatchSize, ConfigType.Int, HttpConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Maximum batch size when using array batch mode")

        // Retry
        .Define(HttpConnectorConfig.RetryMax, ConfigType.Int, HttpConnectorConfig.DefaultRetryMax, Importance.Medium,
            "Maximum number of retries on failure")
        .Define(HttpConnectorConfig.RetryBackoffMs, ConfigType.Long, HttpConnectorConfig.DefaultRetryBackoffMs, Importance.Medium,
            "Initial backoff time between retries in milliseconds (exponential backoff)")

        // Authentication
        .Define(HttpConnectorConfig.AuthType, ConfigType.String, HttpConnectorConfig.AuthTypeNone, Importance.Medium,
            "Authentication type: none, basic, bearer, api_key, or hmac", EditorHint.Select, options: ["none", "basic", "bearer", "api_key", "hmac"])
        .Define(HttpConnectorConfig.AuthUsername, ConfigType.String, "", Importance.Medium,
            "Username for basic authentication")
        .Define(HttpConnectorConfig.AuthPassword, ConfigType.Password, "", Importance.Medium,
            "Password for basic authentication")
        .Define(HttpConnectorConfig.AuthToken, ConfigType.Password, "", Importance.Medium,
            "Token for bearer authentication")
        .Define(HttpConnectorConfig.AuthApiKey, ConfigType.Password, "", Importance.Medium,
            "API key value")
        .Define(HttpConnectorConfig.AuthApiKeyHeader, ConfigType.String, HttpConnectorConfig.DefaultApiKeyHeader, Importance.Low,
            "Header name for API key")
        .Define(HttpConnectorConfig.AuthHmacSecret, ConfigType.Password, "", Importance.Medium,
            "Secret for HMAC request signing")
        .Define(HttpConnectorConfig.AuthHmacHeader, ConfigType.String, HttpConnectorConfig.DefaultSignatureHeader, Importance.Low,
            "Header name for HMAC signature")
        .Define(HttpConnectorConfig.AuthHmacAlgorithm, ConfigType.String, HttpConnectorConfig.DefaultSignatureAlgorithm, Importance.Low,
            "HMAC algorithm: HMAC-SHA256, HMAC-SHA1, or HMAC-SHA512", EditorHint.Select, options: ["HMAC-SHA256", "HMAC-SHA1", "HMAC-SHA512"]);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        // Validate required settings
        if (!config.TryGetValue(HttpConnectorConfig.Url, out var url) || string.IsNullOrEmpty(url))
        {
            throw new ArgumentException($"Missing required config: {HttpConnectorConfig.Url}");
        }

        if (!config.TryGetValue(HttpConnectorConfig.Topics, out var topics) || string.IsNullOrEmpty(topics))
        {
            throw new ArgumentException($"Missing required config: {HttpConnectorConfig.Topics}");
        }

        // Validate auth type and required auth settings
        var authType = config.TryGetValue(HttpConnectorConfig.AuthType, out var at) ? at : HttpConnectorConfig.AuthTypeNone;
        ValidateAuthConfig(authType, config);
    }

    private static void ValidateAuthConfig(string authType, IDictionary<string, string> config)
    {
        switch (authType)
        {
            case HttpConnectorConfig.AuthTypeBasic:
                if (!config.ContainsKey(HttpConnectorConfig.AuthUsername))
                    throw new ArgumentException($"Basic auth requires {HttpConnectorConfig.AuthUsername}");
                break;

            case HttpConnectorConfig.AuthTypeBearer:
                if (!config.ContainsKey(HttpConnectorConfig.AuthToken))
                    throw new ArgumentException($"Bearer auth requires {HttpConnectorConfig.AuthToken}");
                break;

            case HttpConnectorConfig.AuthTypeApiKey:
                if (!config.ContainsKey(HttpConnectorConfig.AuthApiKey))
                    throw new ArgumentException($"API key auth requires {HttpConnectorConfig.AuthApiKey}");
                break;

            case HttpConnectorConfig.AuthTypeHmac:
                if (!config.ContainsKey(HttpConnectorConfig.AuthHmacSecret))
                    throw new ArgumentException($"HMAC auth requires {HttpConnectorConfig.AuthHmacSecret}");
                break;
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // For simplicity, use a single task. Could be extended to partition work.
        return [new Dictionary<string, string>(_config)];
    }
}
