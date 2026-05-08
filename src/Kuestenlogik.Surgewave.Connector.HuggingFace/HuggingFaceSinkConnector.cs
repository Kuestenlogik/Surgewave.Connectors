namespace Kuestenlogik.Surgewave.Connector.HuggingFace;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that processes data using Hugging Face Inference API.
/// Supports sentiment analysis, NER, classification, embeddings, text generation, and more.
/// </summary>
public sealed class HuggingFaceSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(HuggingFaceSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(HuggingFaceConnectorConfig.EndpointConfig, ConfigType.String, HuggingFaceConnectorConfig.DefaultEndpoint, Importance.Medium,
            "Hugging Face Inference API endpoint")
        .Define(HuggingFaceConnectorConfig.ApiKeyConfig, ConfigType.Password, "", Importance.High,
            "Hugging Face API key (required for authentication)")
        .Define(HuggingFaceConnectorConfig.ModelIdConfig, ConfigType.String, "", Importance.High,
            "Model ID to use (e.g., 'distilbert-base-uncased-finetuned-sst-2-english')")
        // Topics
        .Define(HuggingFaceConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(HuggingFaceConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(HuggingFaceConnectorConfig.ModeConfig, ConfigType.String, HuggingFaceConnectorConfig.ModeSentiment, Importance.High,
            "Processing mode: 'sentiment', 'ner', 'classification', 'embeddings', 'text-generation', 'fill-mask', 'question-answering', 'summarization', 'translation'")
        // Input/Output fields
        .Define(HuggingFaceConnectorConfig.InputFieldConfig, ConfigType.String, HuggingFaceConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to process")
        .Define(HuggingFaceConnectorConfig.OutputFieldConfig, ConfigType.String, HuggingFaceConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for response output")
        .Define(HuggingFaceConnectorConfig.EmbeddingsFieldConfig, ConfigType.String, HuggingFaceConnectorConfig.DefaultEmbeddingsField, Importance.Medium,
            "JSON field for embeddings output")
        // Question-Answering fields
        .Define(HuggingFaceConnectorConfig.ContextFieldConfig, ConfigType.String, HuggingFaceConnectorConfig.DefaultContextField, Importance.Low,
            "JSON field containing context for question-answering")
        .Define(HuggingFaceConnectorConfig.QuestionFieldConfig, ConfigType.String, HuggingFaceConnectorConfig.DefaultQuestionField, Importance.Low,
            "JSON field containing question for question-answering")
        // Classification config
        .Define(HuggingFaceConnectorConfig.CandidateLabelsConfig, ConfigType.String, "", Importance.Medium,
            "Comma-separated candidate labels for zero-shot classification")
        .Define(HuggingFaceConnectorConfig.MultiLabelConfig, ConfigType.Boolean, HuggingFaceConnectorConfig.DefaultMultiLabel, Importance.Low,
            "Enable multi-label classification")
        // Text generation config
        .Define(HuggingFaceConnectorConfig.MaxNewTokensConfig, ConfigType.Int, (long)HuggingFaceConnectorConfig.DefaultMaxNewTokens, Importance.Medium,
            "Maximum new tokens to generate")
        .Define(HuggingFaceConnectorConfig.TemperatureConfig, ConfigType.Double, HuggingFaceConnectorConfig.DefaultTemperature, Importance.Low,
            "Sampling temperature")
        .Define(HuggingFaceConnectorConfig.TopKConfig, ConfigType.Int, (long)HuggingFaceConnectorConfig.DefaultTopK, Importance.Low,
            "Top-K sampling")
        .Define(HuggingFaceConnectorConfig.TopPConfig, ConfigType.Double, HuggingFaceConnectorConfig.DefaultTopP, Importance.Low,
            "Top-P (nucleus) sampling")
        .Define(HuggingFaceConnectorConfig.DoSampleConfig, ConfigType.Boolean, HuggingFaceConnectorConfig.DefaultDoSample, Importance.Low,
            "Enable sampling for text generation")
        // Translation config
        .Define(HuggingFaceConnectorConfig.SourceLanguageConfig, ConfigType.String, "", Importance.Low,
            "Source language for translation")
        .Define(HuggingFaceConnectorConfig.TargetLanguageConfig, ConfigType.String, "", Importance.Low,
            "Target language for translation")
        // Batching
        .Define(HuggingFaceConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)HuggingFaceConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch")
        .Define(HuggingFaceConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)HuggingFaceConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(HuggingFaceConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)HuggingFaceConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(HuggingFaceConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)HuggingFaceConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(HuggingFaceConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, HuggingFaceConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(HuggingFaceConnectorConfig.OutputFormatConfig, ConfigType.String, HuggingFaceConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"])
        // Wait for model
        .Define(HuggingFaceConnectorConfig.WaitForModelConfig, ConfigType.Boolean, HuggingFaceConnectorConfig.DefaultWaitForModel, Importance.Low,
            "Wait for model to load if not ready");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate topics
        if (!config.TryGetValue(HuggingFaceConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {HuggingFaceConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(HuggingFaceConnectorConfig.ModeConfig, out var m)
            ? m
            : HuggingFaceConnectorConfig.ModeSentiment;

        var validModes = new[]
        {
            HuggingFaceConnectorConfig.ModeSentiment,
            HuggingFaceConnectorConfig.ModeNer,
            HuggingFaceConnectorConfig.ModeClassification,
            HuggingFaceConnectorConfig.ModeEmbeddings,
            HuggingFaceConnectorConfig.ModeTextGeneration,
            HuggingFaceConnectorConfig.ModeFillMask,
            HuggingFaceConnectorConfig.ModeQuestionAnswering,
            HuggingFaceConnectorConfig.ModeSummarization,
            HuggingFaceConnectorConfig.ModeTranslation
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
