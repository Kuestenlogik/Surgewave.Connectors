namespace Kuestenlogik.Surgewave.Connector.Azure.OpenAI;

/// <summary>
/// Configuration constants for the Azure OpenAI connector.
/// </summary>
public static class AzureOpenAIConnectorConfig
{
    // Connection configuration
    public const string TopicsConfig = "topics";
    public const string EndpointConfig = "azure.openai.endpoint";
    public const string ApiKeyConfig = "azure.openai.api.key";
    public const string DeploymentIdConfig = "azure.openai.deployment.id";

    // Mode configuration
    public const string ModeConfig = "mode";
    public const string ModeChat = "chat";
    public const string ModeEmbeddings = "embeddings";
    public const string ModeDallE = "dalle";
    public const string ModeWhisper = "whisper";

    // Chat completion configuration
    public const string SystemPromptConfig = "system.prompt";
    public const string MaxTokensConfig = "max.tokens";
    public const int DefaultMaxTokens = 4096;
    public const string TemperatureConfig = "temperature";
    public const double DefaultTemperature = 0.7;
    public const string TopPConfig = "top.p";
    public const double DefaultTopP = 1.0;
    public const string FrequencyPenaltyConfig = "frequency.penalty";
    public const double DefaultFrequencyPenalty = 0.0;
    public const string PresencePenaltyConfig = "presence.penalty";
    public const double DefaultPresencePenalty = 0.0;

    // Input/Output configuration
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "response";
    public const string EmbeddingsFieldConfig = "embeddings.field";
    public const string DefaultEmbeddingsField = "embedding";

    // DALL-E configuration
    public const string ImageSizeConfig = "image.size";
    public const string DefaultImageSize = "1024x1024";
    public const string ImageQualityConfig = "image.quality";
    public const string DefaultImageQuality = "standard";
    public const string ImageStyleConfig = "image.style";
    public const string DefaultImageStyle = "vivid";
    public const string ImageCountConfig = "image.count";
    public const int DefaultImageCount = 1;

    // Whisper configuration
    public const string AudioLanguageConfig = "audio.language";
    public const string AudioFormatConfig = "audio.format";
    public const string DefaultAudioFormat = "json";
    public const string WhisperModeTranscribe = "transcribe";
    public const string WhisperModeTranslate = "translate";
    public const string WhisperModeConfig = "whisper.mode";
    public const string DefaultWhisperMode = WhisperModeTranscribe;

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
