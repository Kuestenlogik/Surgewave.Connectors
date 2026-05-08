namespace Kuestenlogik.Surgewave.Connector.Aws.Bedrock;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that processes text using AWS Bedrock foundation models.
/// Supports chat completions and embeddings generation with Claude, Llama, Titan, and other models.
/// </summary>
public sealed class BedrockSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(BedrockSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(BedrockConnectorConfig.RegionConfig, ConfigType.String, "us-east-1", Importance.High,
            "AWS region (e.g., 'us-east-1', 'us-west-2')")
        .Define(BedrockConnectorConfig.AccessKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS access key ID (optional, uses default credentials if not specified)")
        .Define(BedrockConnectorConfig.SecretKeyConfig, ConfigType.Password, "", Importance.Medium,
            "AWS secret access key (optional, uses default credentials if not specified)")
        .Define(BedrockConnectorConfig.EndpointConfig, ConfigType.String, "", Importance.Low,
            "Custom endpoint URL (for LocalStack or testing)")
        // Topics
        .Define(BedrockConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(BedrockConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Model
        .Define(BedrockConnectorConfig.ModelIdConfig, ConfigType.String, BedrockConnectorConfig.DefaultModelId, Importance.High,
            "Bedrock model ID (e.g., 'anthropic.claude-3-5-sonnet-20241022-v2:0', 'meta.llama3-70b-instruct-v1:0')")
        // Mode
        .Define(BedrockConnectorConfig.ModeConfig, ConfigType.String, BedrockConnectorConfig.ModeChat, Importance.High,
            "Processing mode: 'chat' or 'embeddings'", EditorHint.Select, options: ["chat", "embeddings"])
        // Completion config
        .Define(BedrockConnectorConfig.SystemPromptConfig, ConfigType.String, "", Importance.Medium,
            "System prompt for chat completions", EditorHint.Multiline)
        .Define(BedrockConnectorConfig.MaxTokensConfig, ConfigType.Int, (long)BedrockConnectorConfig.DefaultMaxTokens, Importance.Medium,
            "Maximum tokens to generate")
        .Define(BedrockConnectorConfig.TemperatureConfig, ConfigType.Double, BedrockConnectorConfig.DefaultTemperature, Importance.Low,
            "Sampling temperature (0.0-1.0)")
        .Define(BedrockConnectorConfig.TopPConfig, ConfigType.Double, BedrockConnectorConfig.DefaultTopP, Importance.Low,
            "Top-p sampling (0.0-1.0)")
        // Input/Output fields
        .Define(BedrockConnectorConfig.InputFieldConfig, ConfigType.String, BedrockConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to process")
        .Define(BedrockConnectorConfig.OutputFieldConfig, ConfigType.String, BedrockConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for response output")
        .Define(BedrockConnectorConfig.EmbeddingsFieldConfig, ConfigType.String, BedrockConnectorConfig.DefaultEmbeddingsField, Importance.Medium,
            "JSON field for embeddings output")
        // Batching
        .Define(BedrockConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)BedrockConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch")
        .Define(BedrockConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)BedrockConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(BedrockConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)BedrockConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(BedrockConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)BedrockConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(BedrockConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, BedrockConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(BedrockConnectorConfig.OutputFormatConfig, ConfigType.String, BedrockConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate topics
        if (!config.TryGetValue(BedrockConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {BedrockConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(BedrockConnectorConfig.ModeConfig, out var m)
            ? m
            : BedrockConnectorConfig.ModeChat;

        var validModes = new[]
        {
            BedrockConnectorConfig.ModeChat,
            BedrockConnectorConfig.ModeEmbeddings
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
