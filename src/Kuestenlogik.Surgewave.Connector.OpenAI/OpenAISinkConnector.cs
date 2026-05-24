namespace Kuestenlogik.Surgewave.Connector.OpenAI;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that processes records through OpenAI APIs.
/// Supports six modes:
/// - Embeddings: Generate vector embeddings from text fields
/// - Completions: Process messages through chat/completion API for enrichment
/// - Speech: Text-to-speech audio generation (TTS)
/// - Transcription: Speech-to-text transcription (Whisper)
/// - Images: Image generation (DALL-E)
/// - Moderation: Content moderation and safety classification
/// </summary>
public sealed class OpenAISinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(OpenAISinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(OpenAIConnectorConfig.ApiKeyConfig, ConfigType.Password, Importance.High,
            "OpenAI API key (or set OPENAI_API_KEY environment variable)")
        .Define(OpenAIConnectorConfig.BaseUrlConfig, ConfigType.String, "", Importance.Low,
            "Custom base URL for API (for Azure OpenAI or compatible APIs)")
        .Define(OpenAIConnectorConfig.OrganizationConfig, ConfigType.String, "", Importance.Low,
            "OpenAI organization ID")
        .Define(OpenAIConnectorConfig.ProjectConfig, ConfigType.String, "", Importance.Low,
            "OpenAI project ID")
        // Topics
        .Define(OpenAIConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(OpenAIConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(OpenAIConnectorConfig.ModeConfig, ConfigType.String, OpenAIConnectorConfig.ModeEmbeddings, Importance.High,
            "Processing mode: 'embeddings' or 'completions'")
        // Embeddings config
        .Define(OpenAIConnectorConfig.EmbeddingsModelConfig, ConfigType.String, OpenAIConnectorConfig.DefaultEmbeddingsModel, Importance.Medium,
            "Embeddings model (e.g., 'text-embedding-3-small', 'text-embedding-3-large', 'text-embedding-ada-002')")
        .Define(OpenAIConnectorConfig.EmbeddingsDimensionsConfig, ConfigType.Int, 0L, Importance.Low,
            "Output dimensions (0 = use model default)")
        .Define(OpenAIConnectorConfig.InputFieldConfig, ConfigType.String, OpenAIConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to embed or process")
        .Define(OpenAIConnectorConfig.OutputFieldConfig, ConfigType.String, OpenAIConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for output (embedding vector or completion result)")
        // Completions config
        .Define(OpenAIConnectorConfig.CompletionsModelConfig, ConfigType.String, OpenAIConnectorConfig.DefaultCompletionsModel, Importance.Medium,
            "Chat/Completion model (e.g., 'gpt-4o-mini', 'gpt-4o', 'gpt-4-turbo')", EditorHint.Select, options: ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo"])
        .Define(OpenAIConnectorConfig.SystemPromptConfig, ConfigType.String, "", Importance.Medium,
            "System prompt for completions mode", EditorHint.Multiline)
        .Define(OpenAIConnectorConfig.MaxTokensConfig, ConfigType.Int, (long)OpenAIConnectorConfig.DefaultMaxTokens, Importance.Medium,
            "Maximum tokens for completion response")
        .Define(OpenAIConnectorConfig.TemperatureConfig, ConfigType.Double, OpenAIConnectorConfig.DefaultTemperature, Importance.Low,
            "Temperature for completion (0.0 - 2.0)")
        // Speech (TTS) config
        .Define(OpenAIConnectorConfig.SpeechModelConfig, ConfigType.String, OpenAIConnectorConfig.DefaultSpeechModel, Importance.Medium,
            "Text-to-speech model (e.g., 'tts-1', 'tts-1-hd')")
        .Define(OpenAIConnectorConfig.SpeechVoiceConfig, ConfigType.String, OpenAIConnectorConfig.DefaultSpeechVoice, Importance.Medium,
            "Voice for speech synthesis (alloy, echo, fable, onyx, nova, shimmer)")
        .Define(OpenAIConnectorConfig.SpeechFormatConfig, ConfigType.String, OpenAIConnectorConfig.DefaultSpeechFormat, Importance.Low,
            "Audio output format (mp3, opus, aac, flac, wav, pcm)")
        .Define(OpenAIConnectorConfig.SpeechSpeedConfig, ConfigType.Double, OpenAIConnectorConfig.DefaultSpeechSpeed, Importance.Low,
            "Speech speed (0.25 - 4.0)")
        // Transcription (Whisper) config
        .Define(OpenAIConnectorConfig.TranscriptionModelConfig, ConfigType.String, OpenAIConnectorConfig.DefaultTranscriptionModel, Importance.Medium,
            "Whisper model for transcription")
        .Define(OpenAIConnectorConfig.TranscriptionLanguageConfig, ConfigType.String, "", Importance.Low,
            "ISO-639-1 language code for transcription (auto-detect if not set)")
        .Define(OpenAIConnectorConfig.TranscriptionPromptConfig, ConfigType.String, "", Importance.Low,
            "Optional prompt to guide transcription style")
        .Define(OpenAIConnectorConfig.TranscriptionFormatConfig, ConfigType.String, OpenAIConnectorConfig.DefaultTranscriptionFormat, Importance.Low,
            "Transcription output format (json, text, srt, verbose_json, vtt)")
        .Define(OpenAIConnectorConfig.TranscriptionTimestampsConfig, ConfigType.String, "", Importance.Low,
            "Timestamp granularity (word, segment, or empty for none)")
        // Images (DALL-E) config
        .Define(OpenAIConnectorConfig.ImagesModelConfig, ConfigType.String, OpenAIConnectorConfig.DefaultImagesModel, Importance.Medium,
            "Image generation model (dall-e-2, dall-e-3)")
        .Define(OpenAIConnectorConfig.ImagesSizeConfig, ConfigType.String, OpenAIConnectorConfig.DefaultImagesSize, Importance.Medium,
            "Image size (256x256, 512x512, 1024x1024, 1792x1024, 1024x1792)")
        .Define(OpenAIConnectorConfig.ImagesQualityConfig, ConfigType.String, OpenAIConnectorConfig.DefaultImagesQuality, Importance.Low,
            "Image quality (standard, hd)")
        .Define(OpenAIConnectorConfig.ImagesStyleConfig, ConfigType.String, OpenAIConnectorConfig.DefaultImagesStyle, Importance.Low,
            "Image style (vivid, natural)")
        .Define(OpenAIConnectorConfig.ImagesCountConfig, ConfigType.Int, (long)OpenAIConnectorConfig.DefaultImagesCount, Importance.Low,
            "Number of images to generate (1-10)")
        // Moderation config
        .Define(OpenAIConnectorConfig.ModerationModelConfig, ConfigType.String, OpenAIConnectorConfig.DefaultModerationModel, Importance.Medium,
            "Moderation model (omni-moderation-latest, text-moderation-latest, text-moderation-stable)")
        // Batching
        .Define(OpenAIConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)OpenAIConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch for embeddings (max 2048 for OpenAI)")
        .Define(OpenAIConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)OpenAIConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(OpenAIConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)OpenAIConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(OpenAIConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)OpenAIConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(OpenAIConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, OpenAIConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(OpenAIConnectorConfig.OutputFormatConfig, ConfigType.String, OpenAIConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate API key (from config or environment)
        var hasApiKey = config.TryGetValue(OpenAIConnectorConfig.ApiKeyConfig, out var apiKey)
            && !string.IsNullOrEmpty(apiKey);
        var hasEnvApiKey = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("OPENAI_API_KEY"));

        if (!hasApiKey && !hasEnvApiKey)
            throw new ArgumentException($"Missing required config: {OpenAIConnectorConfig.ApiKeyConfig} (or set OPENAI_API_KEY environment variable)");

        // Validate topics
        if (!config.TryGetValue(OpenAIConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {OpenAIConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(OpenAIConnectorConfig.ModeConfig, out var m)
            ? m
            : OpenAIConnectorConfig.ModeEmbeddings;

        if (mode is not (OpenAIConnectorConfig.ModeEmbeddings
                      or OpenAIConnectorConfig.ModeCompletions
                      or OpenAIConnectorConfig.ModeSpeech
                      or OpenAIConnectorConfig.ModeTranscription
                      or OpenAIConnectorConfig.ModeImages
                      or OpenAIConnectorConfig.ModeModeration))
            throw new ArgumentException($"Invalid mode '{mode}'. Must be 'embeddings', 'completions', 'speech', 'transcription', 'images', or 'moderation'");

        // Validate completions mode has system prompt
        if (mode == OpenAIConnectorConfig.ModeCompletions)
        {
            if (!config.TryGetValue(OpenAIConnectorConfig.SystemPromptConfig, out var prompt) || string.IsNullOrEmpty(prompt))
                throw new ArgumentException($"Completions mode requires '{OpenAIConnectorConfig.SystemPromptConfig}' to be specified");
        }

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - OpenAI API handles batching internally
        return [new Dictionary<string, string>(_config)];
    }
}
