namespace Kuestenlogik.Surgewave.Connector.OpenAI;

/// <summary>
/// Configuration constants for the OpenAI connector.
/// </summary>
public static class OpenAIConnectorConfig
{
    // Connection
    public const string ApiKeyConfig = "openai.api.key";
    public const string BaseUrlConfig = "openai.base.url";
    public const string OrganizationConfig = "openai.organization";
    public const string ProjectConfig = "openai.project";

    // Topics
    public const string TopicsConfig = "topics";
    public const string OutputTopicConfig = "output.topic";

    // Output
    public const string WebhookUrlConfig = "webhook.url";

    // Mode
    public const string ModeConfig = "mode";
    public const string ModeEmbeddings = "embeddings";
    public const string ModeCompletions = "completions";
    public const string ModeSpeech = "speech";
    public const string ModeTranscription = "transcription";
    public const string ModeImages = "images";
    public const string ModeModeration = "moderation";

    // Embeddings mode config
    public const string EmbeddingsModelConfig = "embeddings.model";
    public const string DefaultEmbeddingsModel = "text-embedding-3-small";
    public const string EmbeddingsDimensionsConfig = "embeddings.dimensions";
    public const string InputFieldConfig = "input.field";
    public const string DefaultInputField = "text";
    public const string OutputFieldConfig = "output.field";
    public const string DefaultOutputField = "embedding";

    // Completions mode config
    public const string CompletionsModelConfig = "completions.model";
    public const string DefaultCompletionsModel = "gpt-4o-mini";
    public const string SystemPromptConfig = "system.prompt";
    public const string MaxTokensConfig = "max.tokens";
    public const int DefaultMaxTokens = 256;
    public const string TemperatureConfig = "temperature";
    public const double DefaultTemperature = 0.7;

    // Speech (TTS) mode config
    public const string SpeechModelConfig = "speech.model";
    public const string DefaultSpeechModel = "tts-1";
    public const string SpeechVoiceConfig = "speech.voice";
    public const string DefaultSpeechVoice = "alloy";
    public const string SpeechFormatConfig = "speech.format";
    public const string DefaultSpeechFormat = "mp3";
    public const string SpeechSpeedConfig = "speech.speed";
    public const double DefaultSpeechSpeed = 1.0;

    // Transcription (Whisper) mode config
    public const string TranscriptionModelConfig = "transcription.model";
    public const string DefaultTranscriptionModel = "whisper-1";
    public const string TranscriptionLanguageConfig = "transcription.language";
    public const string TranscriptionPromptConfig = "transcription.prompt";
    public const string TranscriptionFormatConfig = "transcription.format";
    public const string DefaultTranscriptionFormat = "json";
    public const string TranscriptionTimestampsConfig = "transcription.timestamps";
    public const string TranscriptionTimestampsWord = "word";
    public const string TranscriptionTimestampsSegment = "segment";

    // Images (DALL-E) mode config
    public const string ImagesModelConfig = "images.model";
    public const string DefaultImagesModel = "dall-e-3";
    public const string ImagesSizeConfig = "images.size";
    public const string DefaultImagesSize = "1024x1024";
    public const string ImagesQualityConfig = "images.quality";
    public const string DefaultImagesQuality = "standard";
    public const string ImagesStyleConfig = "images.style";
    public const string DefaultImagesStyle = "vivid";
    public const string ImagesCountConfig = "images.count";
    public const int DefaultImagesCount = 1;

    // Moderation mode config
    public const string ModerationModelConfig = "moderation.model";
    public const string DefaultModerationModel = "omni-moderation-latest";

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
}
