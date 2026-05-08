namespace Kuestenlogik.Surgewave.Connector.Azure.TextAnalytics;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that processes text using Azure Cognitive Services Text Analytics.
/// Supports sentiment analysis, entity recognition, key phrase extraction, PII detection, and summarization.
/// </summary>
public sealed class TextAnalyticsSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(TextAnalyticsSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(TextAnalyticsConnectorConfig.EndpointConfig, ConfigType.String, Importance.High,
            "Azure Text Analytics endpoint URL")
        .Define(TextAnalyticsConnectorConfig.ApiKeyConfig, ConfigType.Password, "", Importance.High,
            "Azure Text Analytics API key (optional, uses DefaultAzureCredential if not specified)")
        // Topics
        .Define(TextAnalyticsConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(TextAnalyticsConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(TextAnalyticsConnectorConfig.ModeConfig, ConfigType.String, TextAnalyticsConnectorConfig.ModeSentiment, Importance.High,
            "Processing mode: 'sentiment', 'entities', 'key-phrases', 'language-detection', 'pii', 'linked-entities', 'healthcare', 'summarization', 'abstractive-summarization'", EditorHint.Select, options: ["sentiment", "entities", "key-phrases", "language-detection", "pii", "linked-entities", "healthcare", "summarization", "abstractive-summarization"])
        // Input/Output fields
        .Define(TextAnalyticsConnectorConfig.InputFieldConfig, ConfigType.String, TextAnalyticsConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to process")
        .Define(TextAnalyticsConnectorConfig.OutputFieldConfig, ConfigType.String, TextAnalyticsConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for response output")
        .Define(TextAnalyticsConnectorConfig.LanguageConfig, ConfigType.String, TextAnalyticsConnectorConfig.DefaultLanguage, Importance.Medium,
            "Language code for text analysis (e.g., 'en', 'de', 'fr')")
        // PII config
        .Define(TextAnalyticsConnectorConfig.PiiCategoriesConfig, ConfigType.String, "", Importance.Low,
            "Comma-separated PII categories to detect (empty = all)")
        .Define(TextAnalyticsConnectorConfig.PiiDomainConfig, ConfigType.String, TextAnalyticsConnectorConfig.PiiDomainDefault, Importance.Low,
            "PII domain: 'none' or 'phi' (healthcare)", EditorHint.Select, options: ["none", "phi"])
        // Summarization config
        .Define(TextAnalyticsConnectorConfig.MaxSentenceCountConfig, ConfigType.Int, (long)TextAnalyticsConnectorConfig.DefaultMaxSentenceCount, Importance.Medium,
            "Maximum sentences in extractive summary")
        // Batching
        .Define(TextAnalyticsConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)TextAnalyticsConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch")
        .Define(TextAnalyticsConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)TextAnalyticsConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(TextAnalyticsConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)TextAnalyticsConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(TextAnalyticsConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)TextAnalyticsConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(TextAnalyticsConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, TextAnalyticsConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(TextAnalyticsConnectorConfig.OutputFormatConfig, ConfigType.String, TextAnalyticsConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate endpoint
        if (!config.TryGetValue(TextAnalyticsConnectorConfig.EndpointConfig, out _))
            throw new ArgumentException($"Missing required config: {TextAnalyticsConnectorConfig.EndpointConfig}");

        // Validate topics
        if (!config.TryGetValue(TextAnalyticsConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {TextAnalyticsConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(TextAnalyticsConnectorConfig.ModeConfig, out var m)
            ? m
            : TextAnalyticsConnectorConfig.ModeSentiment;

        var validModes = new[]
        {
            TextAnalyticsConnectorConfig.ModeSentiment,
            TextAnalyticsConnectorConfig.ModeEntities,
            TextAnalyticsConnectorConfig.ModeKeyPhrases,
            TextAnalyticsConnectorConfig.ModeLanguageDetection,
            TextAnalyticsConnectorConfig.ModePii,
            TextAnalyticsConnectorConfig.ModeLinkedEntities,
            TextAnalyticsConnectorConfig.ModeHealthcare,
            TextAnalyticsConnectorConfig.ModeSummarization,
            TextAnalyticsConnectorConfig.ModeAbstractiveSummarization
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
