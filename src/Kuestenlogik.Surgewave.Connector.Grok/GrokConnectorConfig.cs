namespace Kuestenlogik.Surgewave.Connector.Grok;

/// <summary>
/// Configuration constants for the xAI Grok connector.
/// </summary>
public static class GrokConnectorConfig
{
    // Connection
    public const string ApiKeyConfig = "grok.api.key";
    public const string BaseUrlConfig = "grok.base.url";
    public const string DefaultBaseUrl = "https://api.x.ai/v1";

    // Topics
    public const string TopicsConfig = "topics";

    // Output
    public const string WebhookUrlConfig = "webhook.url";

    // Mode
    public const string ModeConfig = "mode";
    public const string ModeCompletions = "completions";

    // Completions mode config
    public const string ModelConfig = "model";
    public const string DefaultModel = "grok-3";
    public const string SystemPromptConfig = "system.prompt";
    public const string MaxTokensConfig = "max.tokens";
    public const int DefaultMaxTokens = 1024;
    public const string TemperatureConfig = "temperature";
    public const double DefaultTemperature = 0.7;
    public const string TopPConfig = "top.p";
    public const double DefaultTopP = 1.0;

    // Input/Output fields
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "response";

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

    // Available models
    public const string ModelGrok3 = "grok-3";
    public const string ModelGrok3Mini = "grok-3-mini";
    public const string ModelGrok2 = "grok-2";
    public const string ModelGrok2Mini = "grok-2-mini";
}
