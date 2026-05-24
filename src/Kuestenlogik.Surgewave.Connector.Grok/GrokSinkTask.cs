namespace Kuestenlogik.Surgewave.Connector.Grok;

using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using OpenAI;
using OpenAI.Chat;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that processes records through xAI Grok API.
/// Uses OpenAI-compatible API format.
/// </summary>
public sealed class GrokSinkTask : SinkTask
{
    private ChatClient? _chatClient;
    private string _mode = GrokConnectorConfig.ModeCompletions;
    private string _model = GrokConnectorConfig.DefaultModel;
    private string _systemPrompt = "";
    private int _maxTokens = GrokConnectorConfig.DefaultMaxTokens;
    private double _temperature = GrokConnectorConfig.DefaultTemperature;
    private double _topP = GrokConnectorConfig.DefaultTopP;
    private string _inputField = GrokConnectorConfig.DefaultInputField;
    private string _outputField = GrokConnectorConfig.DefaultOutputField;
    private bool _includeOriginal = GrokConnectorConfig.DefaultIncludeOriginal;
    private string _outputFormat = GrokConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private int _batchSize = GrokConnectorConfig.DefaultBatchSize;
    private int _batchTimeoutMs = GrokConnectorConfig.DefaultBatchTimeoutMs;
    private int _retryMax = GrokConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = GrokConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];
    private DateTime _lastFlush = DateTime.UtcNow;
    private HttpClient? _httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Get API key from config or environment
        var apiKey = config.TryGetValue(GrokConnectorConfig.ApiKeyConfig, out var key) && !string.IsNullOrEmpty(key)
            ? key
            : Environment.GetEnvironmentVariable("XAI_API_KEY");

        if (string.IsNullOrEmpty(apiKey))
            throw new ArgumentException($"Missing required config: {GrokConnectorConfig.ApiKeyConfig}");

        // Get base URL
        var baseUrl = config.TryGetValue(GrokConnectorConfig.BaseUrlConfig, out var url) && !string.IsNullOrEmpty(url)
            ? url
            : GrokConnectorConfig.DefaultBaseUrl;

        // Read model config
        _model = config.TryGetValue(GrokConnectorConfig.ModelConfig, out var model)
            ? model
            : GrokConnectorConfig.DefaultModel;

        // Create OpenAI client with xAI endpoint
        var clientOptions = new OpenAIClientOptions { Endpoint = new Uri(baseUrl) };
        var credential = new System.ClientModel.ApiKeyCredential(apiKey);
        var client = new OpenAIClient(credential, clientOptions);
        _chatClient = client.GetChatClient(_model);

        // Read mode config
        _mode = config.TryGetValue(GrokConnectorConfig.ModeConfig, out var mode)
            ? mode
            : GrokConnectorConfig.ModeCompletions;

        // Read completions config
        _systemPrompt = config.TryGetValue(GrokConnectorConfig.SystemPromptConfig, out var systemPrompt)
            ? systemPrompt
            : "";

        _maxTokens = config.TryGetValue(GrokConnectorConfig.MaxTokensConfig, out var maxTokens) && int.TryParse(maxTokens, out var mt)
            ? mt
            : GrokConnectorConfig.DefaultMaxTokens;

        _temperature = config.TryGetValue(GrokConnectorConfig.TemperatureConfig, out var temp) && double.TryParse(temp, out var t)
            ? t
            : GrokConnectorConfig.DefaultTemperature;

        _topP = config.TryGetValue(GrokConnectorConfig.TopPConfig, out var topP) && double.TryParse(topP, out var tp)
            ? tp
            : GrokConnectorConfig.DefaultTopP;

        // Read input/output config
        _inputField = config.TryGetValue(GrokConnectorConfig.InputFieldConfig, out var inputField)
            ? inputField
            : GrokConnectorConfig.DefaultInputField;

        _outputField = config.TryGetValue(GrokConnectorConfig.OutputFieldConfig, out var outputField)
            ? outputField
            : GrokConnectorConfig.DefaultOutputField;

        // Read batching config
        _batchSize = config.TryGetValue(GrokConnectorConfig.BatchSizeConfig, out var batchSize) && int.TryParse(batchSize, out var bs)
            ? bs
            : GrokConnectorConfig.DefaultBatchSize;

        _batchTimeoutMs = config.TryGetValue(GrokConnectorConfig.BatchTimeoutMsConfig, out var batchTimeout) && int.TryParse(batchTimeout, out var bt)
            ? bt
            : GrokConnectorConfig.DefaultBatchTimeoutMs;

        // Read retry config
        _retryMax = config.TryGetValue(GrokConnectorConfig.RetryMaxConfig, out var retryMax) && int.TryParse(retryMax, out var rm)
            ? rm
            : GrokConnectorConfig.DefaultRetryMax;

        _retryBackoffMs = config.TryGetValue(GrokConnectorConfig.RetryBackoffMsConfig, out var retryBackoff) && int.TryParse(retryBackoff, out var rb)
            ? rb
            : GrokConnectorConfig.DefaultRetryBackoffMs;

        // Read output config
        _includeOriginal = !config.TryGetValue(GrokConnectorConfig.IncludeOriginalConfig, out var includeOriginal)
            || !bool.TryParse(includeOriginal, out var io)
            || io;

        _outputFormat = config.TryGetValue(GrokConnectorConfig.OutputFormatConfig, out var outputFormat)
            ? outputFormat
            : GrokConnectorConfig.FormatMerge;

        _webhookUrl = config.TryGetValue(GrokConnectorConfig.WebhookUrlConfig, out var webhookUrl)
            ? webhookUrl
            : null;

        if (!string.IsNullOrEmpty(_webhookUrl))
        {
            _httpClient = new HttpClient();
        }
    }

    public override void Stop()
    {
        // Flush any remaining records
        if (_buffer.Count > 0)
        {
            FlushBufferAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        _chatClient = null;
        _httpClient?.Dispose();
        _httpClient = null;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken = default)
    {
        _buffer.AddRange(records);

        // Flush if batch size reached or timeout exceeded
        if (_buffer.Count >= _batchSize || (DateTime.UtcNow - _lastFlush).TotalMilliseconds >= _batchTimeoutMs)
        {
            await FlushBufferAsync(cancellationToken);
        }
    }

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        if (_buffer.Count == 0 || _chatClient == null) return;

        var recordsToProcess = _buffer.ToList();
        _buffer.Clear();
        _lastFlush = DateTime.UtcNow;

        // Process completions
        await ProcessCompletionsAsync(recordsToProcess, cancellationToken);
    }

    private async Task ProcessCompletionsAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_chatClient == null) return;

        foreach (var record in records)
        {
            try
            {
                // Extract input text from record
                var inputText = ExtractInputText(record);
                if (string.IsNullOrEmpty(inputText)) continue;

                // Create message request with retry
                string? responseText = null;
                for (var attempt = 0; attempt <= _retryMax; attempt++)
                {
                    try
                    {
                        var messages = new List<ChatMessage>();

                        // Add system message if specified
                        if (!string.IsNullOrEmpty(_systemPrompt))
                        {
                            messages.Add(ChatMessage.CreateSystemMessage(_systemPrompt));
                        }

                        // Add user message
                        messages.Add(ChatMessage.CreateUserMessage(inputText));

                        var options = new ChatCompletionOptions
                        {
                            MaxOutputTokenCount = _maxTokens,
                            Temperature = (float)_temperature,
                            TopP = (float)_topP,
                        };

                        var response = await _chatClient.CompleteChatAsync(messages, options, cancellationToken);

                        // Extract text from response
                        responseText = response.Value.Content[0].Text;
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (responseText == null) continue;

                // Build output
                var output = BuildOutput(record, responseText);

                // Send to webhook or log
                await OutputResultAsync(output, cancellationToken);
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
            }
        }
    }

    private string? ExtractInputText(SinkRecord record)
    {
        if (record.Value == null) return null;

        var valueStr = record.Value is byte[] bytes
            ? System.Text.Encoding.UTF8.GetString(bytes)
            : record.Value.ToString();

        if (string.IsNullOrEmpty(valueStr)) return null;

        try
        {
            var doc = JsonDocument.Parse(valueStr);
            if (doc.RootElement.TryGetProperty(_inputField, out var field))
            {
                return field.GetString();
            }
            // If input field not found, use the whole value as text
            return valueStr;
        }
        catch (JsonException)
        {
            // Not JSON, use raw value
            return valueStr;
        }
    }

    private JsonObject BuildOutput(SinkRecord record, string responseText)
    {
        var output = new JsonObject();

        if (_includeOriginal && _outputFormat == GrokConnectorConfig.FormatMerge)
        {
            // Try to parse original value and merge
            if (record.Value != null)
            {
                var valueStr = record.Value is byte[] bytes
                    ? System.Text.Encoding.UTF8.GetString(bytes)
                    : record.Value.ToString();

                if (!string.IsNullOrEmpty(valueStr))
                {
                    try
                    {
                        var original = JsonNode.Parse(valueStr);
                        if (original is JsonObject originalObj)
                        {
                            foreach (var prop in originalObj)
                            {
                                output[prop.Key] = prop.Value?.DeepClone();
                            }
                        }
                    }
                    catch (JsonException)
                    {
                        // Not JSON, add as raw
                        output["original"] = valueStr;
                    }
                }
            }
        }

        // Add response
        output[_outputField] = responseText;

        // Add metadata
        output["_metadata"] = new JsonObject
        {
            ["topic"] = record.Topic,
            ["partition"] = record.Partition,
            ["offset"] = record.Offset,
            ["timestamp"] = record.Timestamp.ToString("O"),
            ["model"] = _model
        };

        return output;
    }

    private async Task OutputResultAsync(JsonObject output, CancellationToken cancellationToken)
    {
        var json = output.ToJsonString();

        if (!string.IsNullOrEmpty(_webhookUrl) && _httpClient != null)
        {
            await _httpClient.PostAsJsonAsync(_webhookUrl, output, cancellationToken);
        }
        else
        {
            Console.WriteLine(json);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            Stop();
            _httpClient?.Dispose();
            _httpClient = null;
        }
        base.Dispose(disposing);
    }
}
