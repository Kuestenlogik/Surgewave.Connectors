namespace Kuestenlogik.Surgewave.Connector.Azure.OpenAI;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that processes data using Azure OpenAI Service.
/// Supports chat completions, embeddings generation, DALL-E image generation, and Whisper transcription.
/// </summary>
public sealed class AzureOpenAISinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(AzureOpenAISinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(AzureOpenAIConnectorConfig.EndpointConfig, ConfigType.String, Importance.High,
            "Azure OpenAI endpoint URL (e.g., 'https://<resource>.openai.azure.com')")
        .Define(AzureOpenAIConnectorConfig.ApiKeyConfig, ConfigType.Password, "", Importance.High,
            "Azure OpenAI API key (optional, uses DefaultAzureCredential if not specified)")
        .Define(AzureOpenAIConnectorConfig.DeploymentIdConfig, ConfigType.String, Importance.High,
            "Azure OpenAI deployment ID for the model")
        // Topics
        .Define(AzureOpenAIConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(AzureOpenAIConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(AzureOpenAIConnectorConfig.ModeConfig, ConfigType.String, AzureOpenAIConnectorConfig.ModeChat, Importance.High,
            "Processing mode: 'chat', 'embeddings', 'dalle', or 'whisper'", EditorHint.Select, options: ["chat", "embeddings", "dalle", "whisper"])
        // Completion config
        .Define(AzureOpenAIConnectorConfig.SystemPromptConfig, ConfigType.String, "", Importance.Medium,
            "System prompt for chat completions", EditorHint.Multiline)
        .Define(AzureOpenAIConnectorConfig.MaxTokensConfig, ConfigType.Int, (long)AzureOpenAIConnectorConfig.DefaultMaxTokens, Importance.Medium,
            "Maximum tokens to generate")
        .Define(AzureOpenAIConnectorConfig.TemperatureConfig, ConfigType.Double, AzureOpenAIConnectorConfig.DefaultTemperature, Importance.Low,
            "Sampling temperature (0.0-2.0)")
        .Define(AzureOpenAIConnectorConfig.TopPConfig, ConfigType.Double, AzureOpenAIConnectorConfig.DefaultTopP, Importance.Low,
            "Top-p sampling (0.0-1.0)")
        .Define(AzureOpenAIConnectorConfig.FrequencyPenaltyConfig, ConfigType.Double, AzureOpenAIConnectorConfig.DefaultFrequencyPenalty, Importance.Low,
            "Frequency penalty (-2.0 to 2.0)")
        .Define(AzureOpenAIConnectorConfig.PresencePenaltyConfig, ConfigType.Double, AzureOpenAIConnectorConfig.DefaultPresencePenalty, Importance.Low,
            "Presence penalty (-2.0 to 2.0)")
        // Input/Output fields
        .Define(AzureOpenAIConnectorConfig.InputFieldConfig, ConfigType.String, AzureOpenAIConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to process")
        .Define(AzureOpenAIConnectorConfig.OutputFieldConfig, ConfigType.String, AzureOpenAIConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for response output")
        .Define(AzureOpenAIConnectorConfig.EmbeddingsFieldConfig, ConfigType.String, AzureOpenAIConnectorConfig.DefaultEmbeddingsField, Importance.Medium,
            "JSON field for embeddings output")
        // DALL-E config
        .Define(AzureOpenAIConnectorConfig.ImageSizeConfig, ConfigType.String, AzureOpenAIConnectorConfig.DefaultImageSize, Importance.Medium,
            "Image size for DALL-E: '1024x1024', '1024x1792', '1792x1024'", EditorHint.Select, options: ["1024x1024", "1024x1792", "1792x1024"])
        .Define(AzureOpenAIConnectorConfig.ImageQualityConfig, ConfigType.String, AzureOpenAIConnectorConfig.DefaultImageQuality, Importance.Low,
            "Image quality: 'standard' or 'hd'", EditorHint.Select, options: ["standard", "hd"])
        .Define(AzureOpenAIConnectorConfig.ImageStyleConfig, ConfigType.String, AzureOpenAIConnectorConfig.DefaultImageStyle, Importance.Low,
            "Image style: 'vivid' or 'natural'", EditorHint.Select, options: ["vivid", "natural"])
        .Define(AzureOpenAIConnectorConfig.ImageCountConfig, ConfigType.Int, (long)AzureOpenAIConnectorConfig.DefaultImageCount, Importance.Low,
            "Number of images to generate (1-10)")
        // Whisper config
        .Define(AzureOpenAIConnectorConfig.AudioLanguageConfig, ConfigType.String, "", Importance.Medium,
            "Audio language ISO code (optional)")
        .Define(AzureOpenAIConnectorConfig.WhisperModeConfig, ConfigType.String, AzureOpenAIConnectorConfig.DefaultWhisperMode, Importance.Medium,
            "Whisper mode: 'transcribe' or 'translate'", EditorHint.Select, options: ["transcribe", "translate"])
        // Batching
        .Define(AzureOpenAIConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)AzureOpenAIConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch")
        .Define(AzureOpenAIConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)AzureOpenAIConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(AzureOpenAIConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)AzureOpenAIConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(AzureOpenAIConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)AzureOpenAIConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(AzureOpenAIConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, AzureOpenAIConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(AzureOpenAIConnectorConfig.OutputFormatConfig, ConfigType.String, AzureOpenAIConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate endpoint
        if (!config.TryGetValue(AzureOpenAIConnectorConfig.EndpointConfig, out _))
            throw new ArgumentException($"Missing required config: {AzureOpenAIConnectorConfig.EndpointConfig}");

        // Validate deployment
        if (!config.TryGetValue(AzureOpenAIConnectorConfig.DeploymentIdConfig, out _))
            throw new ArgumentException($"Missing required config: {AzureOpenAIConnectorConfig.DeploymentIdConfig}");

        // Validate topics
        if (!config.TryGetValue(AzureOpenAIConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {AzureOpenAIConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(AzureOpenAIConnectorConfig.ModeConfig, out var m)
            ? m
            : AzureOpenAIConnectorConfig.ModeChat;

        var validModes = new[]
        {
            AzureOpenAIConnectorConfig.ModeChat,
            AzureOpenAIConnectorConfig.ModeEmbeddings,
            AzureOpenAIConnectorConfig.ModeDallE,
            AzureOpenAIConnectorConfig.ModeWhisper
        };

        if (!validModes.Contains(mode))
            throw new ArgumentException($"Invalid mode: {mode}. Must be one of: {string.Join(", ", validModes)}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - API handles batching
        return [new Dictionary<string, string>(_config)];
    }
}
