namespace Kuestenlogik.Surgewave.Connector.Grok;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that processes records through xAI Grok API.
/// Uses OpenAI-compatible API endpoints for chat completions.
/// </summary>
public sealed class GrokSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(GrokSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(GrokConnectorConfig.ApiKeyConfig, ConfigType.Password, Importance.High,
            "xAI API key (or set XAI_API_KEY environment variable)")
        .Define(GrokConnectorConfig.BaseUrlConfig, ConfigType.String, GrokConnectorConfig.DefaultBaseUrl, Importance.Low,
            "xAI API base URL (defaults to https://api.x.ai/v1)")
        // Topics
        .Define(GrokConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(GrokConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(GrokConnectorConfig.ModeConfig, ConfigType.String, GrokConnectorConfig.ModeCompletions, Importance.High,
            "Processing mode: 'completions'")
        // Completions config
        .Define(GrokConnectorConfig.ModelConfig, ConfigType.String, GrokConnectorConfig.DefaultModel, Importance.Medium,
            "Grok model (e.g., 'grok-3', 'grok-3-mini', 'grok-2', 'grok-2-mini')")
        .Define(GrokConnectorConfig.SystemPromptConfig, ConfigType.String, "", Importance.Medium,
            "System prompt for completions", EditorHint.Multiline)
        .Define(GrokConnectorConfig.MaxTokensConfig, ConfigType.Int, (long)GrokConnectorConfig.DefaultMaxTokens, Importance.Medium,
            "Maximum tokens for completion response")
        .Define(GrokConnectorConfig.TemperatureConfig, ConfigType.Double, GrokConnectorConfig.DefaultTemperature, Importance.Low,
            "Temperature for completion (0.0 - 2.0)")
        .Define(GrokConnectorConfig.TopPConfig, ConfigType.Double, GrokConnectorConfig.DefaultTopP, Importance.Low,
            "Top P for nucleus sampling (0.0 - 1.0)")
        // Input/Output fields
        .Define(GrokConnectorConfig.InputFieldConfig, ConfigType.String, GrokConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to process")
        .Define(GrokConnectorConfig.OutputFieldConfig, ConfigType.String, GrokConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for output response")
        // Batching
        .Define(GrokConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)GrokConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch")
        .Define(GrokConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)GrokConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(GrokConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)GrokConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(GrokConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)GrokConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(GrokConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, GrokConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(GrokConnectorConfig.OutputFormatConfig, ConfigType.String, GrokConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)", EditorHint.Select, options: ["json", "merge"]);

    public override void Start(IDictionary<string, string> config)
    {
        // Validate API key (from config or environment)
        var hasApiKey = config.TryGetValue(GrokConnectorConfig.ApiKeyConfig, out var apiKey)
            && !string.IsNullOrEmpty(apiKey);
        var hasEnvApiKey = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("XAI_API_KEY"));

        if (!hasApiKey && !hasEnvApiKey)
            throw new ArgumentException($"Missing required config: {GrokConnectorConfig.ApiKeyConfig} (or set XAI_API_KEY environment variable)");

        // Validate topics
        if (!config.TryGetValue(GrokConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {GrokConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(GrokConnectorConfig.ModeConfig, out var m)
            ? m
            : GrokConnectorConfig.ModeCompletions;

        if (mode is not GrokConnectorConfig.ModeCompletions)
            throw new ArgumentException($"Invalid mode '{mode}'. Must be 'completions'");

        // Completions mode requires system prompt
        if (!config.TryGetValue(GrokConnectorConfig.SystemPromptConfig, out var prompt) || string.IsNullOrEmpty(prompt))
            throw new ArgumentException($"Completions mode requires '{GrokConnectorConfig.SystemPromptConfig}' to be specified");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - Grok API handles batching internally
        return [new Dictionary<string, string>(_config)];
    }
}
