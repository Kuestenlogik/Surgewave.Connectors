namespace Kuestenlogik.Surgewave.Connector.Anthropic;

using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using global::Anthropic;
using global::Anthropic.Core;
using global::Anthropic.Models.Messages;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that processes records through Anthropic Claude APIs.
/// </summary>
public sealed class AnthropicSinkTask : SinkTask
{
    private AnthropicClient? _client;
    private string _mode = AnthropicConnectorConfig.ModeCompletions;
    private string _model = AnthropicConnectorConfig.DefaultModel;
    private string _systemPrompt = "";
    private int _maxTokens = AnthropicConnectorConfig.DefaultMaxTokens;
    private double _temperature = AnthropicConnectorConfig.DefaultTemperature;
    private double _topP = AnthropicConnectorConfig.DefaultTopP;
    private int _topK = AnthropicConnectorConfig.DefaultTopK;
    private string _inputField = AnthropicConnectorConfig.DefaultInputField;
    private string _outputField = AnthropicConnectorConfig.DefaultOutputField;
    private bool _includeOriginal = AnthropicConnectorConfig.DefaultIncludeOriginal;
    private string _outputFormat = AnthropicConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private int _batchSize = AnthropicConnectorConfig.DefaultBatchSize;
    private int _batchTimeoutMs = AnthropicConnectorConfig.DefaultBatchTimeoutMs;
    private int _retryMax = AnthropicConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = AnthropicConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];
    private DateTime _lastFlush = DateTime.UtcNow;
    private HttpClient? _httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Get API key from config or environment
        var apiKey = config.TryGetValue(AnthropicConnectorConfig.ApiKeyConfig, out var key) && !string.IsNullOrEmpty(key)
            ? key
            : Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY");

        if (string.IsNullOrEmpty(apiKey))
            throw new ArgumentException($"Missing required config: {AnthropicConnectorConfig.ApiKeyConfig}");

        // Create client with API key
        _client = new AnthropicClient { APIKey = apiKey };

        // Apply custom base URL if specified
        if (config.TryGetValue(AnthropicConnectorConfig.BaseUrlConfig, out var baseUrl) && !string.IsNullOrEmpty(baseUrl))
        {
            var oldClient = _client;
            _client = (AnthropicClient)_client.WithOptions(opts => opts with { BaseUrl = baseUrl });
            oldClient.Dispose();
        }

        // Read mode config
        _mode = config.TryGetValue(AnthropicConnectorConfig.ModeConfig, out var mode)
            ? mode
            : AnthropicConnectorConfig.ModeCompletions;

        // Read completions config
        _model = config.TryGetValue(AnthropicConnectorConfig.ModelConfig, out var model)
            ? model
            : AnthropicConnectorConfig.DefaultModel;

        _systemPrompt = config.TryGetValue(AnthropicConnectorConfig.SystemPromptConfig, out var systemPrompt)
            ? systemPrompt
            : "";

        _maxTokens = config.TryGetValue(AnthropicConnectorConfig.MaxTokensConfig, out var maxTokens) && int.TryParse(maxTokens, out var mt)
            ? mt
            : AnthropicConnectorConfig.DefaultMaxTokens;

        _temperature = config.TryGetValue(AnthropicConnectorConfig.TemperatureConfig, out var temp) && double.TryParse(temp, out var t)
            ? t
            : AnthropicConnectorConfig.DefaultTemperature;

        _topP = config.TryGetValue(AnthropicConnectorConfig.TopPConfig, out var topP) && double.TryParse(topP, out var tp)
            ? tp
            : AnthropicConnectorConfig.DefaultTopP;

        _topK = config.TryGetValue(AnthropicConnectorConfig.TopKConfig, out var topK) && int.TryParse(topK, out var tk)
            ? tk
            : AnthropicConnectorConfig.DefaultTopK;

        // Read input/output config
        _inputField = config.TryGetValue(AnthropicConnectorConfig.InputFieldConfig, out var inputField)
            ? inputField
            : AnthropicConnectorConfig.DefaultInputField;

        _outputField = config.TryGetValue(AnthropicConnectorConfig.OutputFieldConfig, out var outputField)
            ? outputField
            : AnthropicConnectorConfig.DefaultOutputField;

        // Read batching config
        _batchSize = config.TryGetValue(AnthropicConnectorConfig.BatchSizeConfig, out var batchSize) && int.TryParse(batchSize, out var bs)
            ? bs
            : AnthropicConnectorConfig.DefaultBatchSize;

        _batchTimeoutMs = config.TryGetValue(AnthropicConnectorConfig.BatchTimeoutMsConfig, out var batchTimeout) && int.TryParse(batchTimeout, out var bt)
            ? bt
            : AnthropicConnectorConfig.DefaultBatchTimeoutMs;

        // Read retry config
        _retryMax = config.TryGetValue(AnthropicConnectorConfig.RetryMaxConfig, out var retryMax) && int.TryParse(retryMax, out var rm)
            ? rm
            : AnthropicConnectorConfig.DefaultRetryMax;

        _retryBackoffMs = config.TryGetValue(AnthropicConnectorConfig.RetryBackoffMsConfig, out var retryBackoff) && int.TryParse(retryBackoff, out var rb)
            ? rb
            : AnthropicConnectorConfig.DefaultRetryBackoffMs;

        // Read output config
        _includeOriginal = !config.TryGetValue(AnthropicConnectorConfig.IncludeOriginalConfig, out var includeOriginal)
            || !bool.TryParse(includeOriginal, out var io)
            || io;

        _outputFormat = config.TryGetValue(AnthropicConnectorConfig.OutputFormatConfig, out var outputFormat)
            ? outputFormat
            : AnthropicConnectorConfig.FormatMerge;

        _webhookUrl = config.TryGetValue(AnthropicConnectorConfig.WebhookUrlConfig, out var webhookUrl)
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

        _client = null;
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
        if (_buffer.Count == 0 || _client == null) return;

        var recordsToProcess = _buffer.ToList();
        _buffer.Clear();
        _lastFlush = DateTime.UtcNow;

        // Process completions
        await ProcessCompletionsAsync(recordsToProcess, cancellationToken);
    }

    private async Task ProcessCompletionsAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

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
                        var parameters = new MessageCreateParams
                        {
                            MaxTokens = _maxTokens,
                            Messages =
                            [
                                new MessageParam
                                {
                                    Role = Role.User,
                                    Content = inputText,
                                }
                            ],
                            Model = _model,
                            Temperature = _temperature,
                            TopP = _topP,
                            TopK = _topK > 0 ? _topK : null,
                        };

                        // Add system prompt if specified
                        if (!string.IsNullOrEmpty(_systemPrompt))
                        {
                            parameters = parameters with { System = _systemPrompt };
                        }

                        var response = await _client.Messages.Create(parameters, cancellationToken);

                        // Extract text from response
                        responseText = ExtractResponseText(response);
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

    private static string? ExtractResponseText(Message response)
    {
        if (response.Content == null || response.Content.Count == 0)
            return null;

        // Get text from first text block using TryPickText
        foreach (var block in response.Content)
        {
            if (block.TryPickText(out var textBlock))
            {
                return textBlock.Text;
            }
        }

        return null;
    }

    private JsonObject BuildOutput(SinkRecord record, string responseText)
    {
        var output = new JsonObject();

        if (_includeOriginal && _outputFormat == AnthropicConnectorConfig.FormatMerge)
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
            _client?.Dispose();
            _client = null;
            _httpClient?.Dispose();
            _httpClient = null;
        }
        base.Dispose(disposing);
    }
}
