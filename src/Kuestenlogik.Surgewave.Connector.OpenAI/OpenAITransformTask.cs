using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Connect;
using OpenAI;
using OpenAI.Chat;

namespace Kuestenlogik.Surgewave.Connector.OpenAI;

/// <summary>
/// Transform task that calls OpenAI Chat Completions and emits responses to an output topic.
/// Input: raw text or JSON with configurable input field.
/// Output: response text (or JSON with input + response).
/// Creates its own producer if Context.Producer is not available (standalone mode).
/// </summary>
public sealed class OpenAITransformTask : SinkTask
{
    public override string Version => "1.0.0";

    private ChatClient? _chatClient;
    private SurgewaveNativeClient? _ownProducer;
    private string _outputTopic = "";
    private string _systemPrompt = "";
    private string _inputField = "";
    private string _outputFormat = "text";
    private int _maxTokens = 1024;
    private float _temperature = 0.7f;

    public override void Start(IDictionary<string, string> config)
    {
        var apiKey = config.GetValueOrDefault(OpenAIConnectorConfig.ApiKeyConfig, "");
        if (string.IsNullOrEmpty(apiKey))
            apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY") ?? "";

        if (string.IsNullOrEmpty(apiKey))
            throw new ArgumentException("OpenAI API key required (config or OPENAI_API_KEY env var)");

        _outputTopic = config.GetValueOrDefault(OpenAIConnectorConfig.OutputTopicConfig, "") ?? "";
        if (string.IsNullOrEmpty(_outputTopic))
            throw new ArgumentException("output.topic is required");

        var model = config.GetValueOrDefault(OpenAIConnectorConfig.CompletionsModelConfig, "") ?? "";
        _systemPrompt = config.GetValueOrDefault(OpenAIConnectorConfig.SystemPromptConfig, "") ?? "You are a helpful assistant.";
        _inputField = config.GetValueOrDefault(OpenAIConnectorConfig.InputFieldConfig, "") ?? "";
        _outputFormat = config.GetValueOrDefault(OpenAIConnectorConfig.OutputFormatConfig, "") ?? "text";

        if (config.TryGetValue(OpenAIConnectorConfig.MaxTokensConfig, out var maxStr) && int.TryParse(maxStr, out var max))
            _maxTokens = max;
        if (config.TryGetValue(OpenAIConnectorConfig.TemperatureConfig, out var tempStr) && float.TryParse(tempStr, out var temp))
            _temperature = temp;

        var baseUrl = config.GetValueOrDefault(OpenAIConnectorConfig.BaseUrlConfig, "") ?? "";
        var clientOptions = !string.IsNullOrEmpty(baseUrl)
            ? new OpenAIClientOptions { Endpoint = new Uri(baseUrl) }
            : new OpenAIClientOptions();

        var credential = new System.ClientModel.ApiKeyCredential(apiKey);
        var client = new OpenAIClient(credential, clientOptions);
        if (string.IsNullOrEmpty(model)) model = OpenAIConnectorConfig.DefaultCompletionsModel;
        _chatClient = client.GetChatClient(model);

        // Create own producer for standalone mode (when Context.Producer is not injected)
        var bootstrapServers = config.GetValueOrDefault("bootstrap.servers", "") ?? "localhost:9092";
        if (string.IsNullOrEmpty(bootstrapServers)) bootstrapServers = "localhost:9092";
        var parts = bootstrapServers.Split(':');
        _ownProducer = new SurgewaveNativeClient(parts[0], parts.Length > 1 && int.TryParse(parts[1], out var p) ? p : 9092);
        _ownProducer.ConnectAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override void Stop()
    {
        _chatClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing && _ownProducer != null)
        {
            _ownProducer.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _ownProducer = null;
        }
        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_chatClient is null) return;

        foreach (var record in records)
        {
            try
            {
                var inputText = ExtractInput(record);
                if (string.IsNullOrWhiteSpace(inputText))
                    continue;

                var messages = new List<ChatMessage>();
                if (!string.IsNullOrEmpty(_systemPrompt))
                    messages.Add(ChatMessage.CreateSystemMessage(_systemPrompt));
                messages.Add(ChatMessage.CreateUserMessage(inputText));

                var options = new ChatCompletionOptions
                {
                    MaxOutputTokenCount = _maxTokens,
                    Temperature = _temperature
                };

                var response = await _chatClient!.CompleteChatAsync(messages, options, cancellationToken);
                var responseText = response.Value.Content[0].Text ?? "";

                var outputBytes = FormatOutput(inputText, responseText, record);

                if (Context?.Producer != null)
                {
                    await Context.Producer.ProduceAsync(
                        _outputTopic,
                        record.Key,
                        outputBytes,
                        cancellationToken);
                }
                else if (_ownProducer is { IsConnected: true })
                {
                    await _ownProducer.Messaging.SendAsync(
                        _outputTopic, 0,
                        record.Key,
                        outputBytes,
                        cancellationToken);
                }
            }
            catch (Exception ex)
            {
                Context?.RaiseError?.Invoke(ex);
            }
        }
    }

    private string ExtractInput(SinkRecord record)
    {
        if (record.Value is null || record.Value.Length == 0)
            return "";

        var raw = Encoding.UTF8.GetString(record.Value);

        // If no input field configured, use raw value as text
        if (string.IsNullOrEmpty(_inputField))
            return raw;

        // Try to extract field from JSON
        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.TryGetProperty(_inputField, out var field))
                return field.GetString() ?? "";
        }
        catch (JsonException)
        {
            // Not JSON — use raw text
        }

        return raw;
    }

    private byte[] FormatOutput(string input, string response, SinkRecord record)
    {
        if (_outputFormat == "json")
        {
            var json = JsonSerializer.Serialize(new
            {
                input,
                response,
                model = "openai",
                topic = record.Topic,
                timestamp = DateTimeOffset.UtcNow
            });
            return Encoding.UTF8.GetBytes(json);
        }

        // Default: plain text response
        return Encoding.UTF8.GetBytes(response);
    }
}
