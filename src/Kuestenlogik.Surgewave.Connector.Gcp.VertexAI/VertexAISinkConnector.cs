namespace Kuestenlogik.Surgewave.Connector.Gcp.VertexAI;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that sends records to GCP Vertex AI (Gemini models).
/// Supports chat completions and embeddings generation.
/// </summary>
public sealed class VertexAISinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(VertexAISinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(VertexAIConnectorConfig.ProjectIdConfig, ConfigType.String, Importance.High,
            "GCP project ID (or set GOOGLE_CLOUD_PROJECT environment variable)")
        .Define(VertexAIConnectorConfig.LocationConfig, ConfigType.String, VertexAIConnectorConfig.DefaultLocation, Importance.Medium,
            "GCP location for Vertex AI (e.g., us-central1, europe-west1)")
        .Define(VertexAIConnectorConfig.CredentialsJsonConfig, ConfigType.Password, "", Importance.Medium,
            "GCP service account JSON credentials (inline)")
        .Define(VertexAIConnectorConfig.CredentialsPathConfig, ConfigType.String, "", Importance.Medium,
            "Path to GCP service account JSON credentials file")
        // Topics
        .Define(VertexAIConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(VertexAIConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(VertexAIConnectorConfig.ModeConfig, ConfigType.String, VertexAIConnectorConfig.ModeCompletions, Importance.High,
            "Processing mode: 'completions' or 'embeddings'", EditorHint.Select, options: ["chat", "embeddings"])
        // Completions config
        .Define(VertexAIConnectorConfig.ModelConfig, ConfigType.String, VertexAIConnectorConfig.DefaultModel, Importance.Medium,
            "Gemini model (e.g., 'gemini-2.0-flash', 'gemini-1.5-pro', 'gemini-1.5-flash')")
        .Define(VertexAIConnectorConfig.SystemPromptConfig, ConfigType.String, "", Importance.Medium,
            "System prompt for completions", EditorHint.Multiline)
        .Define(VertexAIConnectorConfig.MaxTokensConfig, ConfigType.Int, (long)VertexAIConnectorConfig.DefaultMaxTokens, Importance.Medium,
            "Maximum tokens for completion response")
        .Define(VertexAIConnectorConfig.TemperatureConfig, ConfigType.Double, VertexAIConnectorConfig.DefaultTemperature, Importance.Low,
            "Temperature for completion (0.0 - 2.0)")
        .Define(VertexAIConnectorConfig.TopPConfig, ConfigType.Double, VertexAIConnectorConfig.DefaultTopP, Importance.Low,
            "Top P for nucleus sampling (0.0 - 1.0)")
        .Define(VertexAIConnectorConfig.TopKConfig, ConfigType.Int, (long)VertexAIConnectorConfig.DefaultTopK, Importance.Low,
            "Top K for sampling")
        // Embeddings config
        .Define(VertexAIConnectorConfig.EmbeddingsModelConfig, ConfigType.String, VertexAIConnectorConfig.DefaultEmbeddingsModel, Importance.Medium,
            "Model for embeddings (e.g., 'text-embedding-005', 'text-multilingual-embedding-002')")
        .Define(VertexAIConnectorConfig.EmbeddingsDimensionsConfig, ConfigType.Int, (long)VertexAIConnectorConfig.DefaultEmbeddingsDimensions, Importance.Low,
            "Dimensions of generated embeddings")
        .Define(VertexAIConnectorConfig.EmbeddingsFieldConfig, ConfigType.String, VertexAIConnectorConfig.DefaultEmbeddingsField, Importance.Medium,
            "JSON field for embeddings output")
        // Input/Output fields
        .Define(VertexAIConnectorConfig.InputFieldConfig, ConfigType.String, VertexAIConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to process")
        .Define(VertexAIConnectorConfig.OutputFieldConfig, ConfigType.String, VertexAIConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for output response")
        // Batching
        .Define(VertexAIConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)VertexAIConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch")
        .Define(VertexAIConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)VertexAIConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(VertexAIConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)VertexAIConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(VertexAIConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)VertexAIConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(VertexAIConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, VertexAIConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(VertexAIConnectorConfig.OutputFormatConfig, ConfigType.String, VertexAIConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate project ID (from config or environment)
        var hasProjectId = config.TryGetValue(VertexAIConnectorConfig.ProjectIdConfig, out var projectId)
            && !string.IsNullOrEmpty(projectId);
        var hasEnvProjectId = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT"));

        if (!hasProjectId && !hasEnvProjectId)
            throw new ArgumentException($"Missing required config: {VertexAIConnectorConfig.ProjectIdConfig} (or set GOOGLE_CLOUD_PROJECT environment variable)");

        // Validate topics
        if (!config.TryGetValue(VertexAIConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {VertexAIConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(VertexAIConnectorConfig.ModeConfig, out var m)
            ? m
            : VertexAIConnectorConfig.ModeCompletions;

        if (mode != VertexAIConnectorConfig.ModeCompletions && mode != VertexAIConnectorConfig.ModeEmbeddings)
            throw new ArgumentException($"Invalid mode: {mode}. Must be '{VertexAIConnectorConfig.ModeCompletions}' or '{VertexAIConnectorConfig.ModeEmbeddings}'.");

        // Completions mode requires system prompt
        if (mode == VertexAIConnectorConfig.ModeCompletions)
        {
            if (!config.TryGetValue(VertexAIConnectorConfig.SystemPromptConfig, out var prompt) || string.IsNullOrEmpty(prompt))
                throw new ArgumentException($"Completions mode requires '{VertexAIConnectorConfig.SystemPromptConfig}' to be specified");
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
        // Single task - Vertex AI API handles batching internally
        return [new Dictionary<string, string>(_config)];
    }
}
