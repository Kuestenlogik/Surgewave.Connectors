namespace Kuestenlogik.Surgewave.Connector.Gcp.Language;

/// <summary>
/// Configuration constants for the Google Cloud Natural Language connector.
/// </summary>
public static class LanguageConnectorConfig
{
    // Connection
    public const string ProjectIdConfig = "gcp.project.id";
    public const string CredentialsJsonConfig = "gcp.credentials.json";
    public const string CredentialsPathConfig = "gcp.credentials.path";

    // Topics
    public const string TopicsConfig = "topics";

    // Output
    public const string WebhookUrlConfig = "webhook.url";

    // Mode
    public const string ModeConfig = "mode";
    public const string ModeSentiment = "sentiment";
    public const string ModeEntities = "entities";
    public const string ModeSyntax = "syntax";
    public const string ModeClassify = "classify";
    public const string ModeAll = "all";

    // Language
    public const string LanguageConfig = "language";
    public const string DefaultLanguage = "en";

    // Input/Output fields
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "analysis";

    // Batching
    public const string BatchSizeConfig = "batch.size";
    public const int DefaultBatchSize = 10;
    public const string BatchTimeoutMsConfig = "batch.timeout.ms";
    public const int DefaultBatchTimeoutMs = 5000;

    // Retry
    public const string RetryMaxConfig = "retry.max";
    public const int DefaultRetryMax = 3;
    public const string RetryBackoffMsConfig = "retry.backoff.ms";
    public const int DefaultRetryBackoffMs = 1000;

    // Output
    public const string IncludeOriginalConfig = "include.original";
    public const bool DefaultIncludeOriginal = true;
    public const string OutputFormatConfig = "output.format";
    public const string FormatJson = "json";
    public const string FormatMerge = "merge";
}
