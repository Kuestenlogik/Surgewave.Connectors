namespace Kuestenlogik.Surgewave.Connector.Anthropic;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that processes records through Anthropic Claude APIs.
/// Supports chat completions with Claude 3.5/Opus/Haiku/Sonnet 4/Opus 4 models.
/// </summary>
public sealed class AnthropicSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(AnthropicSinkTask);

    public override ConfigDef Config => new ConfigDef()
        // Connection configs
        .Define(AnthropicConnectorConfig.ApiKeyConfig, ConfigType.Password, Importance.High,
            "Anthropic API key (or set ANTHROPIC_API_KEY environment variable)")
        .Define(AnthropicConnectorConfig.BaseUrlConfig, ConfigType.String, "", Importance.Low,
            "Custom base URL for API (optional)")
        // Topics
        .Define(AnthropicConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated list of input topics to consume", EditorHint.Topic)
        // Output
        .Define(AnthropicConnectorConfig.WebhookUrlConfig, ConfigType.String, "", Importance.Medium,
            "Webhook URL to POST results (optional, logs to console if not set)")
        // Mode
        .Define(AnthropicConnectorConfig.ModeConfig, ConfigType.String, AnthropicConnectorConfig.ModeCompletions, Importance.High,
            "Processing mode: 'completions'", EditorHint.Select, options: ["completions"])
        // Completions config
        .Define(AnthropicConnectorConfig.ModelConfig, ConfigType.String, AnthropicConnectorConfig.DefaultModel, Importance.Medium,
            "Claude model (e.g., 'claude-sonnet-4-20250514', 'claude-opus-4-20250514', 'claude-3-5-sonnet-latest')", EditorHint.Select, options: ["claude-opus-4-20250514", "claude-sonnet-4-20250514", "claude-haiku-4-5-20251001"])
        .Define(AnthropicConnectorConfig.SystemPromptConfig, ConfigType.String, "", Importance.Medium,
            "System prompt for completions", EditorHint.Multiline)
        .Define(AnthropicConnectorConfig.MaxTokensConfig, ConfigType.Int, (long)AnthropicConnectorConfig.DefaultMaxTokens, Importance.Medium,
            "Maximum tokens for completion response")
        .Define(AnthropicConnectorConfig.TemperatureConfig, ConfigType.Double, AnthropicConnectorConfig.DefaultTemperature, Importance.Low,
            "Temperature for completion (0.0 - 1.0)")
        .Define(AnthropicConnectorConfig.TopPConfig, ConfigType.Double, AnthropicConnectorConfig.DefaultTopP, Importance.Low,
            "Top P for nucleus sampling (0.0 - 1.0)")
        .Define(AnthropicConnectorConfig.TopKConfig, ConfigType.Int, (long)AnthropicConnectorConfig.DefaultTopK, Importance.Low,
            "Top K for sampling (0 = disabled)")
        // Input/Output fields
        .Define(AnthropicConnectorConfig.InputFieldConfig, ConfigType.String, AnthropicConnectorConfig.DefaultInputField, Importance.Medium,
            "JSON field containing text to process")
        .Define(AnthropicConnectorConfig.OutputFieldConfig, ConfigType.String, AnthropicConnectorConfig.DefaultOutputField, Importance.Medium,
            "JSON field for output response")
        // Batching
        .Define(AnthropicConnectorConfig.BatchSizeConfig, ConfigType.Int, (long)AnthropicConnectorConfig.DefaultBatchSize, Importance.Medium,
            "Number of records to batch")
        .Define(AnthropicConnectorConfig.BatchTimeoutMsConfig, ConfigType.Int, (long)AnthropicConnectorConfig.DefaultBatchTimeoutMs, Importance.Low,
            "Maximum time to wait for batch to fill")
        // Retry
        .Define(AnthropicConnectorConfig.RetryMaxConfig, ConfigType.Int, (long)AnthropicConnectorConfig.DefaultRetryMax, Importance.Low,
            "Maximum retry attempts on failure")
        .Define(AnthropicConnectorConfig.RetryBackoffMsConfig, ConfigType.Int, (long)AnthropicConnectorConfig.DefaultRetryBackoffMs, Importance.Low,
            "Backoff time between retries in milliseconds")
        // Output
        .Define(AnthropicConnectorConfig.IncludeOriginalConfig, ConfigType.Boolean, AnthropicConnectorConfig.DefaultIncludeOriginal, Importance.Low,
            "Include original message fields in output")
        .Define(AnthropicConnectorConfig.OutputFormatConfig, ConfigType.String, AnthropicConnectorConfig.FormatMerge, Importance.Low,
            "Output format: 'json' (new document) or 'merge' (merge with original)");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate API key (from config or environment)
        var hasApiKey = config.TryGetValue(AnthropicConnectorConfig.ApiKeyConfig, out var apiKey)
            && !string.IsNullOrEmpty(apiKey);
        var hasEnvApiKey = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY"));

        if (!hasApiKey && !hasEnvApiKey)
            throw new ArgumentException($"Missing required config: {AnthropicConnectorConfig.ApiKeyConfig} (or set ANTHROPIC_API_KEY environment variable)");

        // Validate topics
        if (!config.TryGetValue(AnthropicConnectorConfig.TopicsConfig, out _))
            throw new ArgumentException($"Missing required config: {AnthropicConnectorConfig.TopicsConfig}");

        // Validate mode
        var mode = config.TryGetValue(AnthropicConnectorConfig.ModeConfig, out var m)
            ? m
            : AnthropicConnectorConfig.ModeCompletions;

        if (mode is not AnthropicConnectorConfig.ModeCompletions)
            throw new ArgumentException($"Invalid mode '{mode}'. Must be 'completions'");

        // Completions mode requires system prompt
        if (!config.TryGetValue(AnthropicConnectorConfig.SystemPromptConfig, out var prompt) || string.IsNullOrEmpty(prompt))
            throw new ArgumentException($"Completions mode requires '{AnthropicConnectorConfig.SystemPromptConfig}' to be specified");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - Anthropic API handles batching internally
        return [new Dictionary<string, string>(_config)];
    }
}
