namespace Kuestenlogik.Surgewave.Connector.DeepL;

/// <summary>
/// Configuration constants for the DeepL connector.
/// </summary>
public static class DeepLConnectorConfig
{
    // Topics
    public const string TopicsConfig = "topics";

    // API configuration
    public const string ApiKeyConfig = "deepl.api.key";
    public const string ServerUrlConfig = "deepl.server.url";
    public const string DefaultServerUrl = "";

    // Mode configuration
    public const string ModeConfig = "mode";
    public const string ModeTranslate = "translate";
    public const string ModeDetectLanguage = "detect-language";
    public const string ModeUsage = "usage";

    // Input/Output fields
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "result";

    // Translation configuration
    public const string SourceLanguageConfig = "source.language";
    public const string DefaultSourceLanguage = "";  // Auto-detect
    public const string TargetLanguageConfig = "target.language";
    public const string DefaultTargetLanguage = "EN-US";

    // Formality configuration
    public const string FormalityConfig = "formality";
    public const string FormalityDefault = "default";
    public const string FormalityMore = "more";
    public const string FormalityLess = "less";
    public const string FormalityPreferMore = "prefer_more";
    public const string FormalityPreferLess = "prefer_less";

    // Context configuration
    public const string ContextConfig = "context";
    public const string DefaultContext = "";

    // Glossary configuration
    public const string GlossaryIdConfig = "glossary.id";

    // Tag handling configuration
    public const string TagHandlingConfig = "tag.handling";
    public const string TagHandlingXml = "xml";
    public const string TagHandlingHtml = "html";

    // Preserve formatting
    public const string PreserveFormattingConfig = "preserve.formatting";
    public const bool DefaultPreserveFormatting = false;

    // Split sentences
    public const string SplitSentencesConfig = "split.sentences";
    public const string SplitSentencesNone = "none";
    public const string SplitSentencesAll = "all";
    public const string SplitSentencesPunctuation = "punctuation";

    // Batching configuration
    public const string BatchSizeConfig = "batch.size";
    public const int DefaultBatchSize = 25;  // DeepL API allows up to 50 texts per request
    public const string BatchTimeoutMsConfig = "batch.timeout.ms";
    public const int DefaultBatchTimeoutMs = 5000;

    // Retry configuration
    public const string RetryMaxConfig = "retry.max";
    public const int DefaultRetryMax = 3;
    public const string RetryBackoffMsConfig = "retry.backoff.ms";
    public const int DefaultRetryBackoffMs = 1000;

    // Output format configuration
    public const string IncludeOriginalConfig = "include.original";
    public const bool DefaultIncludeOriginal = true;
    public const string IncludeDetectedLanguageConfig = "include.detected.language";
    public const bool DefaultIncludeDetectedLanguage = true;
    public const string OutputFormatConfig = "output.format";
    public const string FormatJson = "json";
    public const string FormatMerge = "merge";

    // Webhook output (optional)
    public const string WebhookUrlConfig = "webhook.url";
}
