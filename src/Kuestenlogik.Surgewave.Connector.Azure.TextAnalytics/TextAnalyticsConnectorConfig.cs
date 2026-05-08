namespace Kuestenlogik.Surgewave.Connector.Azure.TextAnalytics;

/// <summary>
/// Configuration constants for the Azure Text Analytics connector.
/// </summary>
public static class TextAnalyticsConnectorConfig
{
    // Connection configuration
    public const string TopicsConfig = "topics";
    public const string EndpointConfig = "azure.text.analytics.endpoint";
    public const string ApiKeyConfig = "azure.text.analytics.api.key";

    // Mode configuration
    public const string ModeConfig = "mode";
    public const string ModeSentiment = "sentiment";
    public const string ModeEntities = "entities";
    public const string ModeKeyPhrases = "key-phrases";
    public const string ModeLanguageDetection = "language-detection";
    public const string ModePii = "pii";
    public const string ModeLinkedEntities = "linked-entities";
    public const string ModeHealthcare = "healthcare";
    public const string ModeSummarization = "summarization";
    public const string ModeAbstractiveSummarization = "abstractive-summarization";

    // Input/Output configuration
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "result";
    public const string LanguageConfig = "language";
    public const string DefaultLanguage = "en";

    // PII configuration
    public const string PiiCategoriesConfig = "pii.categories";
    public const string PiiDomainConfig = "pii.domain";
    public const string PiiDomainDefault = "none";
    public const string PiiDomainHealthcare = "phi";

    // Summarization configuration
    public const string MaxSentenceCountConfig = "max.sentence.count";
    public const int DefaultMaxSentenceCount = 3;

    // Batching configuration
    public const string BatchSizeConfig = "batch.size";
    public const int DefaultBatchSize = 10;
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
    public const string OutputFormatConfig = "output.format";
    public const string FormatJson = "json";
    public const string FormatMerge = "merge";

    // Webhook configuration
    public const string WebhookUrlConfig = "webhook.url";
}
