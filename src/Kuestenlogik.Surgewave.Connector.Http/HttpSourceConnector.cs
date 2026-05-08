using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Http;

/// <summary>
/// A source connector that supports two modes:
/// - Poll: Periodically fetches data from an HTTP endpoint
/// - Webhook: Receives push events via registered HTTP endpoints
/// </summary>
public sealed class HttpSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(HttpSourceTask);

    public override ConfigDef Config => new ConfigDef()
        // Common settings
        .Define(HttpConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Topic to write records to", EditorHint.Topic)
        .Define(HttpConnectorConfig.SourceMode, ConfigType.String, HttpConnectorConfig.SourceModePoll, Importance.Medium,
            "Source mode: 'poll' (fetch from URL) or 'webhook' (receive push events)", EditorHint.Select, options: ["poll", "webhook"])

        // Poll mode settings
        .Define(HttpConnectorConfig.Url, ConfigType.String, "", Importance.High,
            "HTTP URL to poll for data (required for poll mode)")
        .Define(HttpConnectorConfig.PollIntervalMs, ConfigType.Long, HttpConnectorConfig.DefaultPollIntervalMs, Importance.Medium,
            "Poll interval in milliseconds")
        .Define(HttpConnectorConfig.Method, ConfigType.String, "GET", Importance.Medium,
            "HTTP method (GET or POST)")
        .Define(HttpConnectorConfig.Headers, ConfigType.String, "", Importance.Low,
            "HTTP headers as key=value pairs separated by semicolons", EditorHint.Multiline)
        .Define(HttpConnectorConfig.ResponseMode, ConfigType.String, HttpConnectorConfig.ResponseModeRaw, Importance.Medium,
            "Response mode: 'raw' (entire response as one record) or 'json_array' (each array element as a record)", EditorHint.Select, options: ["raw", "json_array"])

        // Webhook mode settings
        .Define(HttpConnectorConfig.WebhookPath, ConfigType.String, HttpConnectorConfig.DefaultWebhookPath, Importance.Medium,
            "Webhook endpoint path (use {name} for connector name)")
        .Define(HttpConnectorConfig.WebhookSecret, ConfigType.Password, "", Importance.High,
            "Secret for HMAC signature validation")
        .Define(HttpConnectorConfig.WebhookSignatureHeader, ConfigType.String, HttpConnectorConfig.DefaultSignatureHeader, Importance.Low,
            "Header containing the webhook signature")
        .Define(HttpConnectorConfig.WebhookSignatureAlgorithm, ConfigType.String, HttpConnectorConfig.DefaultSignatureAlgorithm, Importance.Low,
            "Signature algorithm: HMAC-SHA256, HMAC-SHA1, or HMAC-SHA512", EditorHint.Select, options: ["HMAC-SHA256", "HMAC-SHA1", "HMAC-SHA512"])
        .Define(HttpConnectorConfig.WebhookValidateTimestamp, ConfigType.Boolean, false, Importance.Low,
            "Validate timestamp to prevent replay attacks")
        .Define(HttpConnectorConfig.WebhookTimestampHeader, ConfigType.String, HttpConnectorConfig.DefaultTimestampHeader, Importance.Low,
            "Header containing the request timestamp")
        .Define(HttpConnectorConfig.WebhookTimestampToleranceMs, ConfigType.Long, HttpConnectorConfig.DefaultTimestampToleranceMs, Importance.Low,
            "Maximum age of webhook request in milliseconds");

    private IDictionary<string, string> _config = new Dictionary<string, string>();
    private string _sourceMode = HttpConnectorConfig.SourceModePoll;

    public override void Start(IDictionary<string, string> config)
    {
        _config = config;

        _sourceMode = config.TryGetValue(HttpConnectorConfig.SourceMode, out var mode)
            ? mode : HttpConnectorConfig.SourceModePoll;

        // Validate required settings based on mode
        if (_sourceMode == HttpConnectorConfig.SourceModePoll)
        {
            if (!config.TryGetValue(HttpConnectorConfig.Url, out var url) || string.IsNullOrEmpty(url))
            {
                throw new ArgumentException($"Missing required config for poll mode: {HttpConnectorConfig.Url}");
            }
        }

        if (!config.TryGetValue(HttpConnectorConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException($"Missing required config: {HttpConnectorConfig.Topic}");
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // HTTP source only supports a single task
        var taskConfig = new Dictionary<string, string>(_config);

        // Ensure the connector name is available to the task
        if (!taskConfig.ContainsKey("name") && _config.TryGetValue("name", out var name))
        {
            taskConfig["name"] = name;
        }

        return [taskConfig];
    }
}
