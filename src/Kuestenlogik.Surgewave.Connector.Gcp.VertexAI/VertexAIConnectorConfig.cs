namespace Kuestenlogik.Surgewave.Connector.Gcp.VertexAI;

/// <summary>
/// Configuration constants for the GCP Vertex AI connector.
/// </summary>
public static class VertexAIConnectorConfig
{
    // Connection
    public const string ProjectIdConfig = "gcp.project.id";
    public const string LocationConfig = "gcp.location";
    public const string DefaultLocation = "us-central1";
    public const string CredentialsJsonConfig = "gcp.credentials.json";
    public const string CredentialsPathConfig = "gcp.credentials.path";

    // Topics
    public const string TopicsConfig = "topics";

    // Output
    public const string WebhookUrlConfig = "webhook.url";

    // Mode
    public const string ModeConfig = "mode";
    public const string ModeCompletions = "completions";
    public const string ModeEmbeddings = "embeddings";

    // Completions mode config
    public const string ModelConfig = "model";
    public const string DefaultModel = "gemini-2.0-flash";
    public const string SystemPromptConfig = "system.prompt";
    public const string MaxTokensConfig = "max.tokens";
    public const int DefaultMaxTokens = 1024;
    public const string TemperatureConfig = "temperature";
    public const double DefaultTemperature = 1.0;
    public const string TopPConfig = "top.p";
    public const double DefaultTopP = 0.95;
    public const string TopKConfig = "top.k";
    public const int DefaultTopK = 40;

    // Embeddings mode config
    public const string EmbeddingsModelConfig = "embeddings.model";
    public const string DefaultEmbeddingsModel = "text-embedding-005";
    public const string EmbeddingsDimensionsConfig = "embeddings.dimensions";
    public const int DefaultEmbeddingsDimensions = 768;

    // Input/Output fields
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "response";
    public const string EmbeddingsFieldConfig = "embeddings.field";
    public const string DefaultEmbeddingsField = "embedding";

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
    public const string ModelGemini20Flash = "gemini-2.0-flash";
    public const string ModelGemini20FlashLite = "gemini-2.0-flash-lite";
    public const string ModelGemini15Pro = "gemini-1.5-pro";
    public const string ModelGemini15Flash = "gemini-1.5-flash";
    public const string ModelGeminiPro = "gemini-pro";
}
