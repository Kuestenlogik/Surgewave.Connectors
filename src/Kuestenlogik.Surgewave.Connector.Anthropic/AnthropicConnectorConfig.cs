namespace Kuestenlogik.Surgewave.Connector.Anthropic;

/// <summary>
/// Configuration constants for the Anthropic Claude connector.
/// </summary>
public static class AnthropicConnectorConfig
{
    // Connection
    public const string ApiKeyConfig = "anthropic.api.key";
    public const string BaseUrlConfig = "anthropic.base.url";

    // Topics
    public const string TopicsConfig = "topics";

    // Output
    public const string WebhookUrlConfig = "webhook.url";

    // Mode
    public const string ModeConfig = "mode";
    public const string ModeCompletions = "completions";

    // Completions mode config
    public const string ModelConfig = "model";
    public const string DefaultModel = "claude-sonnet-4-20250514";
    public const string SystemPromptConfig = "system.prompt";
    public const string MaxTokensConfig = "max.tokens";
    public const int DefaultMaxTokens = 1024;
    public const string TemperatureConfig = "temperature";
    public const double DefaultTemperature = 1.0;
    public const string TopPConfig = "top.p";
    public const double DefaultTopP = 1.0;
    public const string TopKConfig = "top.k";
    public const int DefaultTopK = 0;

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
    public const string ModelClaude35Sonnet = "claude-3-5-sonnet-latest";
    public const string ModelClaude35Haiku = "claude-3-5-haiku-latest";
    public const string ModelClaude3Opus = "claude-3-opus-latest";
    public const string ModelClaudeSonnet4 = "claude-sonnet-4-20250514";
    public const string ModelClaudeOpus4 = "claude-opus-4-20250514";
}
