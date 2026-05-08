using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.OpenAI;

/// <summary>
/// Transform connector that reads messages from input topics, sends them to OpenAI,
/// and writes the response to an output topic. Ideal for chat pipelines:
///   TopicSource → OpenAITransform → TopicSink
/// </summary>
[ConnectorMetadata(
    Name = "OpenAI Transform",
    Description = "Reads messages, calls OpenAI Chat Completions, writes responses to output topic",
    Tags = "ai,openai,gpt,transform,chat,llm",
    Icon = "Transform",
    Author = "Kuestenlogik")]
public sealed class OpenAITransformConnector : SinkConnector
{
    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(OpenAITransformTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(OpenAIConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Input topics to consume (comma-separated)", EditorHint.Topic)
        .Define(OpenAIConnectorConfig.OutputTopicConfig, ConfigType.String, Importance.High,
            "Output topic for responses", EditorHint.Topic)
        .Define(OpenAIConnectorConfig.ApiKeyConfig, ConfigType.Password, "", Importance.High,
            "OpenAI API key (or set OPENAI_API_KEY env var)")
        .Define(OpenAIConnectorConfig.CompletionsModelConfig, ConfigType.String,
            OpenAIConnectorConfig.DefaultCompletionsModel, Importance.High,
            "Chat model", EditorHint.Select,
            options: ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo", "gpt-3.5-turbo"])
        .Define(OpenAIConnectorConfig.SystemPromptConfig, ConfigType.String,
            "You are a helpful assistant.", Importance.High,
            "System prompt for the assistant", EditorHint.Multiline)
        .Define(OpenAIConnectorConfig.MaxTokensConfig, ConfigType.Int,
            1024, Importance.Medium,
            "Maximum output tokens")
        .Define(OpenAIConnectorConfig.TemperatureConfig, ConfigType.Double,
            0.7, Importance.Medium,
            "Sampling temperature (0.0 - 2.0)")
        .Define(OpenAIConnectorConfig.BaseUrlConfig, ConfigType.String,
            "", Importance.Low,
            "Custom API endpoint (Azure OpenAI, compatible APIs)")
        .Define(OpenAIConnectorConfig.InputFieldConfig, ConfigType.String,
            "", Importance.Low,
            "JSON field to extract as input (empty = use raw value as text)")
        .Define(OpenAIConnectorConfig.OutputFormatConfig, ConfigType.String,
            "text", Importance.Low,
            "Output format", EditorHint.Select, options: ["text", "json"]);

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        foreach (var kvp in config) _config[kvp.Key] = kvp.Value;
    }

    public override void Stop() { }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
        => [new Dictionary<string, string>(_config)];
}
