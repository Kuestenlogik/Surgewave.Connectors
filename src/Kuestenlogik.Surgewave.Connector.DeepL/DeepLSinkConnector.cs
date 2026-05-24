namespace Kuestenlogik.Surgewave.Connector.DeepL;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that processes text using DeepL translation API.
/// Supports translation, language detection, and usage monitoring.
/// </summary>
public sealed class DeepLSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(DeepLSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // API configuration
        .Define(DeepLConnectorConfig.ApiKeyConfig, ConfigType.Password, Importance.High,
            "DeepL API authentication key")
        .Define(DeepLConnectorConfig.ServerUrlConfig, ConfigType.String, DeepLConnectorConfig.DefaultServerUrl, Importance.Low,
            "DeepL API server URL (optional, leave empty for default)")
        // Topics
        .Define(DeepLConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(DeepLConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(DeepLConnectorConfig.ModeConfig, ConfigType.String, DeepLConnectorConfig.ModeTranslate, Importance.High,
            "Processing mode: 'translate', 'detect-language', 'usage'", EditorHint.Select, options: ["translate", "detect-language", "usage"])
        // Input/Output fields
        .Define(DeepLConnectorConfig.InputFieldConfig, ConfigType.String, DeepLConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to process")
        .Define(DeepLConnectorConfig.OutputFieldConfig, ConfigType.String, DeepLConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for response output")
        // Translation configuration
        .Define(DeepLConnectorConfig.SourceLanguageConfig, ConfigType.String, DeepLConnectorConfig.DefaultSourceLanguage, Importance.Medium,
            "Source language code (e.g., 'EN', 'DE', 'FR'). Leave empty for auto-detection")
        .Define(DeepLConnectorConfig.TargetLanguageConfig, ConfigType.String, DeepLConnectorConfig.DefaultTargetLanguage, Importance.High,
            "Target language code (e.g., 'EN-US', 'EN-GB', 'DE', 'FR')")
        // Formality
        .Define(DeepLConnectorConfig.FormalityConfig, ConfigType.String, DeepLConnectorConfig.FormalityDefault, Importance.Low,
            "Formality level: 'default', 'more', 'less', 'prefer_more', 'prefer_less'", EditorHint.Select, options: ["default", "more", "less", "prefer_more", "prefer_less"])
        // Context
        .Define(DeepLConnectorConfig.ContextConfig, ConfigType.String, DeepLConnectorConfig.DefaultContext, Importance.Low,
            "Additional context for translation (up to 10000 characters)")
        // Glossary
        .Define(DeepLConnectorConfig.GlossaryIdConfig, ConfigType.String, "", Importance.Low,
            "Glossary ID to use for translation")
        // Tag handling
        .Define(DeepLConnectorConfig.TagHandlingConfig, ConfigType.String, "", Importance.Low,
            "Tag handling mode: 'xml' or 'html'")
        // Preserve formatting
        .Define(DeepLConnectorConfig.PreserveFormattingConfig, ConfigType.Boolean, DeepLConnectorConfig.DefaultPreserveFormatting, Importance.Low,
            "Preserve formatting in translated text")
        // Split sentences
        .Define(DeepLConnectorConfig.SplitSentencesConfig, ConfigType.String, DeepLConnectorConfig.SplitSentencesAll, Importance.Low,
            "Sentence splitting: 'none', 'all', 'punctuation'", EditorHint.Select, options: ["none", "all", "punctuation"])
        // Batching
        .Define(DeepLConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)DeepLConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch for translation")
        .Define(DeepLConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)DeepLConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(DeepLConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)DeepLConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(DeepLConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)DeepLConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output format
        .Define(DeepLConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, DeepLConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(DeepLConnectorConfig.IncludeDetectedLanguageConfig, ConfigType.Boolean, DeepLConnectorConfig.DefaultIncludeDetectedLanguage, Importance.Low,
            "Include detected source language in output")
        .Define(DeepLConnectorConfig.OutputFormatConfig, ConfigType.String, DeepLConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate API key
        if (!config.TryGetValue(DeepLConnectorConfig.ApiKeyConfig, out var apiKey) || string.IsNullOrWhiteSpace(apiKey))
            throw new ArgumentException($"Missing required config: {DeepLConnectorConfig.ApiKeyConfig}");

        // Validate topics
        if (!config.TryGetValue(DeepLConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {DeepLConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(DeepLConnectorConfig.ModeConfig, out var m)
            ? m
            : DeepLConnectorConfig.ModeTranslate;

        var validModes = new[]
        {
            DeepLConnectorConfig.ModeTranslate,
            DeepLConnectorConfig.ModeDetectLanguage,
            DeepLConnectorConfig.ModeUsage
        };

        if (!validModes.Contains(mode))
            throw new ArgumentException($"Invalid mode: {mode}. Must be one of: {string.Join(", ", validModes)}");

        // Validate target language for translate mode
        if (mode == DeepLConnectorConfig.ModeTranslate)
        {
            if (!config.TryGetValue(DeepLConnectorConfig.TargetLanguageConfig, out var targetLang) || string.IsNullOrWhiteSpace(targetLang))
            {
                // Use default target language
                config[DeepLConnectorConfig.TargetLanguageConfig] = DeepLConnectorConfig.DefaultTargetLanguage;
            }
        }

        // Validate formality if specified
        if (config.TryGetValue(DeepLConnectorConfig.FormalityConfig, out var formality) && !string.IsNullOrWhiteSpace(formality))
        {
            var validFormalities = new[]
            {
                DeepLConnectorConfig.FormalityDefault,
                DeepLConnectorConfig.FormalityMore,
                DeepLConnectorConfig.FormalityLess,
                DeepLConnectorConfig.FormalityPreferMore,
                DeepLConnectorConfig.FormalityPreferLess
            };

            if (!validFormalities.Contains(formality))
                throw new ArgumentException($"Invalid formality: {formality}. Must be one of: {string.Join(", ", validFormalities)}");
        }

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
