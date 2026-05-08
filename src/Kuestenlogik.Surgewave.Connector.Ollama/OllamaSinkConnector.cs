namespace Kuestenlogik.Surgewave.Connector.Ollama;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that processes records through Ollama local LLM APIs.
/// Supports two modes:
/// - Embeddings: Generate vector embeddings from text fields using local models
/// - Completions: Process messages through local LLMs for enrichment
/// </summary>
public sealed class OllamaSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(OllamaSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(OllamaConnectorConfig.BaseUrlConfig, ConfigType.String, OllamaConnectorConfig.DefaultBaseUrl, Importance.High,
            "Ollama server base URL (default: http://localhost:11434)")
        // Topics
        .Define(OllamaConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(OllamaConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(OllamaConnectorConfig.ModeConfig, ConfigType.String, OllamaConnectorConfig.ModeEmbeddings, Importance.High,
            "Processing mode: 'embeddings' or 'completions'", EditorHint.Select, options: ["embeddings", "completions"])
        // Embeddings config
        .Define(OllamaConnectorConfig.EmbeddingsModelConfig, ConfigType.String, OllamaConnectorConfig.DefaultEmbeddingsModel, Importance.Medium,
            "Embeddings model (e.g., 'nomic-embed-text', 'mxbai-embed-large', 'all-minilm')")
        .Define(OllamaConnectorConfig.InputFieldConfig, ConfigType.String, OllamaConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to embed or process")
        .Define(OllamaConnectorConfig.OutputFieldConfig, ConfigType.String, OllamaConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for output (embedding vector or completion result)")
        // Completions config
        .Define(OllamaConnectorConfig.CompletionsModelConfig, ConfigType.String, OllamaConnectorConfig.DefaultCompletionsModel, Importance.Medium,
            "Chat/Completion model (e.g., 'llama3', 'mistral', 'qwen2', 'gemma2')")
        .Define(OllamaConnectorConfig.SystemPromptConfig, ConfigType.String, "", Importance.Medium,
            "System prompt for completions mode", EditorHint.Multiline)
        .Define(OllamaConnectorConfig.MaxTokensConfig, ConfigType.Int, (long)OllamaConnectorConfig.DefaultMaxTokens, Importance.Medium,
            "Maximum tokens for completion response (num_predict in Ollama)")
        .Define(OllamaConnectorConfig.TemperatureConfig, ConfigType.Double, OllamaConnectorConfig.DefaultTemperature, Importance.Low,
            "Temperature for completion (0.0 - 2.0)")
        // Batching
        .Define(OllamaConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)OllamaConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch before processing")
        .Define(OllamaConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)OllamaConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(OllamaConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)OllamaConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(OllamaConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)OllamaConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(OllamaConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, OllamaConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(OllamaConnectorConfig.OutputFormatConfig, ConfigType.String, OllamaConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"])
        // Ollama-specific
        .Define(OllamaConnectorConfig.KeepAliveConfig, ConfigType.String, OllamaConnectorConfig.DefaultKeepAlive, Importance.Low,
            "How long to keep model loaded in memory (e.g., '5m', '1h', '-1' for indefinite)");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate topics
        if (!config.TryGetValue(OllamaConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrEmpty(topics))
            throw new ArgumentException($"Missing required config: {OllamaConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(OllamaConnectorConfig.ModeConfig, out var m)
            ? m
            : OllamaConnectorConfig.ModeEmbeddings;

        if (mode is not (OllamaConnectorConfig.ModeEmbeddings or OllamaConnectorConfig.ModeCompletions))
            throw new ArgumentException($"Invalid mode '{mode}'. Must be 'embeddings' or 'completions'");

        // Validate completions mode has system prompt (optional but recommended)
        // Note: Unlike OpenAI, we don't require system prompt for Ollama completions

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - Ollama handles processing locally
        return [new Dictionary<string, string>(_config)];
    }
}
