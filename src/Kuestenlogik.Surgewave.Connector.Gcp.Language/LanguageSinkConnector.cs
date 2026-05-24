namespace Kuestenlogik.Surgewave.Connector.Gcp.Language;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that analyzes text using Google Cloud Natural Language API.
/// Supports sentiment analysis, entity extraction, syntax analysis, and content classification.
/// </summary>
public sealed class LanguageSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(LanguageSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(LanguageConnectorConfig.ProjectIdConfig, ConfigType.String, "", Importance.Medium,
            "GCP project ID (optional, uses default if not specified)")
        .Define(LanguageConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.Medium,
            "GCP service account JSON credentials (inline)")
        .Define(LanguageConnectorConfig.CredentialsPathConfig, ConfigType.String, "", Importance.Medium,
            "Path to GCP service account JSON credentials file")
        // Topics
        .Define(LanguageConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(LanguageConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(LanguageConnectorConfig.ModeConfig, ConfigType.String, LanguageConnectorConfig.ModeSentiment, Importance.High,
            "Analysis mode: 'sentiment', 'entities', 'syntax', 'classify', or 'all'", EditorHint.Select, options: ["sentiment", "entities", "syntax", "classify", "all"])
        // Language
        .Define(LanguageConnectorConfig.LanguageConfig, ConfigType.String, LanguageConnectorConfig.DefaultLanguage, Importance.Low,
            "Document language code (e.g., 'en', 'es', 'fr', 'de')")
        // Input/Output fields
        .Define(LanguageConnectorConfig.InputFieldConfig, ConfigType.String, LanguageConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to analyze")
        .Define(LanguageConnectorConfig.OutputFieldConfig, ConfigType.String, LanguageConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for analysis output")
        // Batching
        .Define(LanguageConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)LanguageConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch")
        .Define(LanguageConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)LanguageConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(LanguageConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)LanguageConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(LanguageConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)LanguageConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(LanguageConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, LanguageConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(LanguageConnectorConfig.OutputFormatConfig, ConfigType.String, LanguageConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate topics
        if (!config.TryGetValue(LanguageConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {LanguageConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(LanguageConnectorConfig.ModeConfig, out var m)
            ? m
            : LanguageConnectorConfig.ModeSentiment;

        var validModes = new[]
        {
            LanguageConnectorConfig.ModeSentiment,
            LanguageConnectorConfig.ModeEntities,
            LanguageConnectorConfig.ModeSyntax,
            LanguageConnectorConfig.ModeClassify,
            LanguageConnectorConfig.ModeAll
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
