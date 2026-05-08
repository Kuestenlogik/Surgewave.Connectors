namespace Kuestenlogik.Surgewave.Connector.Aws.Comprehend;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that analyzes text using Amazon Comprehend.
/// Supports sentiment analysis, entity extraction, key phrase detection, language detection, PII detection, and syntax analysis.
/// </summary>
public sealed class ComprehendSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(ComprehendSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(ComprehendConnectorConfig.RegionConfig, ConfigType.String, "us-east-1", Importance.High,
            "AWS region (e.g., 'us-east-1', 'eu-west-1')")
        .Define(ComprehendConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS access key ID (optional, uses default credentials if not specified)")
        .Define(ComprehendConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS secret access key (optional, uses default credentials if not specified)")
        .Define(ComprehendConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low,
            "Custom endpoint URL (for LocalStack or testing)")
        // Topics
        .Define(ComprehendConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(ComprehendConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(ComprehendConnectorConfig.ModeConfig, ConfigType.String, ComprehendConnectorConfig.ModeSentiment, Importance.High,
            "Analysis mode: 'sentiment', 'entities', 'key_phrases', 'language', 'pii', 'syntax', or 'all'", EditorHint.Select, options: ["sentiment", "entities", "key_phrases", "language", "pii", "syntax", "all"])
        // Language
        .Define(ComprehendConnectorConfig.LanguageConfig, ConfigType.String, ComprehendConnectorConfig.DefaultLanguage, Importance.Low,
            "Document language code (e.g., 'en', 'es', 'fr', 'de')")
        // Input/Output fields
        .Define(ComprehendConnectorConfig.InputFieldConfig, ConfigType.String, ComprehendConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to analyze")
        .Define(ComprehendConnectorConfig.OutputFieldConfig, ConfigType.String, ComprehendConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for analysis output")
        // Batching
        .Define(ComprehendConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)ComprehendConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch (max 25 for Comprehend)")
        .Define(ComprehendConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)ComprehendConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(ComprehendConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)ComprehendConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(ComprehendConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)ComprehendConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(ComprehendConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, ComprehendConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(ComprehendConnectorConfig.OutputFormatConfig, ConfigType.String, ComprehendConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate topics
        if (!config.TryGetValue(ComprehendConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {ComprehendConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(ComprehendConnectorConfig.ModeConfig, out var m)
            ? m
            : ComprehendConnectorConfig.ModeSentiment;

        var validModes = new[]
        {
            ComprehendConnectorConfig.ModeSentiment,
            ComprehendConnectorConfig.ModeEntities,
            ComprehendConnectorConfig.ModeKeyPhrases,
            ComprehendConnectorConfig.ModeLanguage,
            ComprehendConnectorConfig.ModePii,
            ComprehendConnectorConfig.ModeSyntax,
            ComprehendConnectorConfig.ModeAll
        };

        if (!validModes.Contains(mode))
            throw new ArgumentException($"Invalid mode: {mode}. Must be one of: {string.Join(", ", validModes)}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - API handles batching
        return [new Dictionary<string, string>(_config)];
    }
}
