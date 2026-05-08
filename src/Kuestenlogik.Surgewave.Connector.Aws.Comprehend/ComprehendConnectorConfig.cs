namespace Kuestenlogik.Surgewave.Connector.Aws.Comprehend;

/// <summary>
/// Configuration constants for the AWS Comprehend connector.
/// </summary>
public static class ComprehendConnectorConfig
{
    // Connection configuration
    public const string TopicsConfig = "topics";
    public const string RegionConfig = "aws.region";
    public const string AccessKeyConfig = "aws.access.key.id";
    public const string SecretKeyConfig = "aws.secret.access.key";
    public const string EndpointConfig = "aws.endpoint";

    // Mode configuration
    public const string ModeConfig = "mode";
    public const string ModeSentiment = "sentiment";
    public const string ModeEntities = "entities";
    public const string ModeKeyPhrases = "key_phrases";
    public const string ModeLanguage = "language";
    public const string ModePii = "pii";
    public const string ModeSyntax = "syntax";
    public const string ModeAll = "all";

    // Language configuration
    public const string LanguageConfig = "language";
    public const string DefaultLanguage = "en";

    // Input/Output configuration
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "analysis";

    // Batching configuration
    public const string BatchSizeConfig = "batch.size";
    public const int DefaultBatchSize = 25; // AWS Comprehend batch limit
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
