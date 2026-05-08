namespace Kuestenlogik.Surgewave.Connector.Ollama;

/// <summary>
/// Configuration constants for the Ollama connector.
/// Ollama provides local LLM inference with an OpenAI-compatible API.
/// </summary>
public static class OllamaConnectorConfig
{
    // Connection
    public const string BaseUrlConfig = "ollama.base.url";
    public const string DefaultBaseUrl = "http://localhost:11434";

    // Topics
    public const string TopicsConfig = "topics";

    // Output
    public const string WebhookUrlConfig = "webhook.url";

    // Mode
    public const string ModeConfig = "mode";
    public const string ModeEmbeddings = "embeddings";
    public const string ModeCompletions = "completions";

    // Embeddings mode config
    public const string EmbeddingsModelConfig = "embeddings.model";
    public const string DefaultEmbeddingsModel = "nomic-embed-text";
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "embedding";

    // Completions mode config
    public const string CompletionsModelConfig = "completions.model";
    public const string DefaultCompletionsModel = "llama3";
    public const string SystemPromptConfig = "system.prompt";
    public const string MaxTokensConfig = "max.tokens";
    public const int DefaultMaxTokens = 256;
    public const string TemperatureConfig = "temperature";
    public const double DefaultTemperature = 0.7;

    // Batching
    public const string BatchSizeConfig = "batch.size";
    public const int DefaultBatchSize = 100;
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

    // Ollama-specific
    public const string KeepAliveConfig = "ollama.keep.alive";
    public const string DefaultKeepAlive = "5m";
}
