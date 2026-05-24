namespace Kuestenlogik.Surgewave.Connector.Ollama;

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;
using OllamaSharp;
using OllamaSharp.Models.Chat;

/// <summary>
/// Task that processes records through Ollama local LLM APIs using OllamaSharp.
/// Embeddings mode generates vectors that can be logged or sent to a webhook.
/// Completions mode enriches messages with AI-generated content.
/// </summary>
public sealed class OllamaSinkTask : SinkTask
{
    private OllamaApiClient? _client;
    private string _mode = OllamaConnectorConfig.ModeEmbeddings;
    private string _embeddingsModel = OllamaConnectorConfig.DefaultEmbeddingsModel;
    private string _completionsModel = OllamaConnectorConfig.DefaultCompletionsModel;
    private string _systemPrompt = "";
    private int _maxTokens = OllamaConnectorConfig.DefaultMaxTokens;
    private float _temperature = (float)OllamaConnectorConfig.DefaultTemperature;
    private string _inputField = OllamaConnectorConfig.DefaultInputField;
    private string _outputField = OllamaConnectorConfig.DefaultOutputField;
    private string _webhookUrl = "";
    private string _keepAlive = OllamaConnectorConfig.DefaultKeepAlive;
    private bool _includeOriginal = OllamaConnectorConfig.DefaultIncludeOriginal;
    private int _batchSize = OllamaConnectorConfig.DefaultBatchSize;
    private int _retryMax = OllamaConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = OllamaConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];
    private HttpClient? _webhookClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var baseUrl = GetConfig(config, OllamaConnectorConfig.BaseUrlConfig, OllamaConnectorConfig.DefaultBaseUrl);
        _mode = GetConfig(config, OllamaConnectorConfig.ModeConfig, OllamaConnectorConfig.ModeEmbeddings);
        _embeddingsModel = GetConfig(config, OllamaConnectorConfig.EmbeddingsModelConfig, OllamaConnectorConfig.DefaultEmbeddingsModel);
        _completionsModel = GetConfig(config, OllamaConnectorConfig.CompletionsModelConfig, OllamaConnectorConfig.DefaultCompletionsModel);
        _systemPrompt = GetConfig(config, OllamaConnectorConfig.SystemPromptConfig, "");
        _maxTokens = int.Parse(GetConfig(config, OllamaConnectorConfig.MaxTokensConfig, OllamaConnectorConfig.DefaultMaxTokens.ToString()));
        _temperature = float.Parse(GetConfig(config, OllamaConnectorConfig.TemperatureConfig, OllamaConnectorConfig.DefaultTemperature.ToString()));
        _inputField = GetConfig(config, OllamaConnectorConfig.InputFieldConfig, OllamaConnectorConfig.DefaultInputField);
        _outputField = GetConfig(config, OllamaConnectorConfig.OutputFieldConfig, OllamaConnectorConfig.DefaultOutputField);
        _webhookUrl = GetConfig(config, OllamaConnectorConfig.WebhookUrlConfig, "");
        _keepAlive = GetConfig(config, OllamaConnectorConfig.KeepAliveConfig, OllamaConnectorConfig.DefaultKeepAlive);
        _includeOriginal = bool.Parse(GetConfig(config, OllamaConnectorConfig.IncludeOriginalConfig, OllamaConnectorConfig.DefaultIncludeOriginal.ToString()));
        _batchSize = int.Parse(GetConfig(config, OllamaConnectorConfig.BatchSizeConfig, OllamaConnectorConfig.DefaultBatchSize.ToString()));
        _retryMax = int.Parse(GetConfig(config, OllamaConnectorConfig.RetryMaxConfig, OllamaConnectorConfig.DefaultRetryMax.ToString()));
        _retryBackoffMs = int.Parse(GetConfig(config, OllamaConnectorConfig.RetryBackoffMsConfig, OllamaConnectorConfig.DefaultRetryBackoffMs.ToString()));

        _client = new OllamaApiClient(new Uri(baseUrl));

        if (!string.IsNullOrEmpty(_webhookUrl))
        {
            _webhookClient = new HttpClient();
        }
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0)
            return;

        _buffer.AddRange(records);

        if (_buffer.Count >= _batchSize)
        {
            await FlushBufferAsync(cancellationToken);
        }
    }

    public override async Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        if (_buffer.Count > 0)
        {
            await FlushBufferAsync(cancellationToken);
        }
    }

    private async Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        var batch = _buffer.ToList();
        _buffer.Clear();

        if (_mode == OllamaConnectorConfig.ModeEmbeddings)
        {
            await ProcessEmbeddingsAsync(batch, cancellationToken);
        }
        else
        {
            await ProcessCompletionsAsync(batch, cancellationToken);
        }
    }

    private async Task ProcessEmbeddingsAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null)
            return;

        foreach (var record in records)
        {
            var rawValue = Encoding.UTF8.GetString(record.Value);
            JsonNode? original = null;
            string? inputText = null;

            try
            {
                original = JsonNode.Parse(rawValue);
                inputText = original?[_inputField]?.GetValue<string>();
            }
            catch (JsonException)
            {
                inputText = rawValue;
            }

            if (string.IsNullOrEmpty(inputText))
                continue;

            // Call Ollama embeddings API with retry
            float[]? embedding = null;
            for (int attempt = 0; attempt <= _retryMax; attempt++)
            {
                try
                {
                    var response = await _client.EmbedAsync(new OllamaSharp.Models.EmbedRequest
                    {
                        Model = _embeddingsModel,
                        Input = [inputText],
                        KeepAlive = _keepAlive
                    }, cancellationToken);

                    if (response.Embeddings.Count > 0)
                    {
                        embedding = response.Embeddings[0];
                    }
                    break;
                }
                catch (HttpRequestException) when (attempt < _retryMax)
                {
                    await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                }
            }

            if (embedding == null)
                continue;

            var output = CreateOutputJson(original, embedding, rawValue);
            await SendOutputAsync(output, record, cancellationToken);
        }
    }

    private async Task ProcessCompletionsAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null)
            return;

        foreach (var record in records)
        {
            var rawValue = Encoding.UTF8.GetString(record.Value);
            JsonNode? original = null;
            string? inputText = null;

            try
            {
                original = JsonNode.Parse(rawValue);
                inputText = original?[_inputField]?.GetValue<string>();
            }
            catch (JsonException)
            {
                inputText = rawValue;
            }

            if (string.IsNullOrEmpty(inputText))
                continue;

            // Call Ollama chat API with retry
            string? completion = null;
            for (int attempt = 0; attempt <= _retryMax; attempt++)
            {
                try
                {
                    var messages = new List<Message>();

                    if (!string.IsNullOrEmpty(_systemPrompt))
                    {
                        messages.Add(new Message(ChatRole.System, _systemPrompt));
                    }

                    messages.Add(new Message(ChatRole.User, inputText));

                    var chat = new ChatRequest
                    {
                        Model = _completionsModel,
                        Messages = messages,
                        Stream = false,
                        KeepAlive = _keepAlive,
                        Options = new OllamaSharp.Models.RequestOptions
                        {
                            NumPredict = _maxTokens,
                            Temperature = _temperature
                        }
                    };

                    // ChatAsync returns IAsyncEnumerable - collect all chunks
                    var responseBuilder = new StringBuilder();
                    await foreach (var chunk in _client.ChatAsync(chat, cancellationToken))
                    {
                        if (chunk?.Message?.Content != null)
                        {
                            responseBuilder.Append(chunk.Message.Content);
                        }
                    }
                    completion = responseBuilder.ToString();
                    break;
                }
                catch (HttpRequestException) when (attempt < _retryMax)
                {
                    await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                }
            }

            if (completion == null)
                continue;

            var output = CreateOutputJson(original, completion, rawValue);
            await SendOutputAsync(output, record, cancellationToken);
        }
    }

    private JsonNode CreateOutputJson(JsonNode? original, object result, string rawValue)
    {
        if (original != null)
        {
            // Merge result into original
            original[_outputField] = JsonValue.Create(result);
            return original;
        }
        else
        {
            // Create new document
            var output = new JsonObject
            {
                [_outputField] = JsonValue.Create(result)
            };

            if (_includeOriginal)
            {
                try
                {
                    output["original"] = JsonNode.Parse(rawValue);
                }
                catch (JsonException)
                {
                    output["original"] = rawValue;
                }
            }

            return output;
        }
    }

    private async Task SendOutputAsync(JsonNode output, SinkRecord sourceRecord, CancellationToken cancellationToken)
    {
        var json = output.ToJsonString();

        // Send to webhook if configured
        if (_webhookClient != null && !string.IsNullOrEmpty(_webhookUrl))
        {
            try
            {
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                await _webhookClient.PostAsync(new Uri(_webhookUrl), content, cancellationToken);
            }
            catch (HttpRequestException)
            {
                // Log error but don't fail the task
                Context?.RaiseError?.Invoke(new Exception($"Failed to send to webhook: {_webhookUrl}"));
            }
        }
        else
        {
            // Default: log to console for debugging
            Console.WriteLine($"[Ollama] {sourceRecord.Topic}:{sourceRecord.Partition}:{sourceRecord.Offset} -> {json[..Math.Min(200, json.Length)]}...");
        }
    }

    public override void Stop()
    {
        _buffer.Clear();
        _client = null;
        _webhookClient?.Dispose();
        _webhookClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _client?.Dispose();
            _client = null;
            _webhookClient?.Dispose();
            _webhookClient = null;
        }
        base.Dispose(disposing);
    }

    private static string GetConfig(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;
}
