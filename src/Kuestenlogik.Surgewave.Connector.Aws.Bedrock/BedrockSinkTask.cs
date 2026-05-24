namespace Kuestenlogik.Surgewave.Connector.Aws.Bedrock;

using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Amazon;
using Amazon.BedrockRuntime;
using Amazon.BedrockRuntime.Model;
using Amazon.Runtime;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that processes text using AWS Bedrock foundation models.
/// </summary>
public sealed class BedrockSinkTask : SinkTask
{
    private AmazonBedrockRuntimeClient? _client;
    private string _modelId = BedrockConnectorConfig.DefaultModelId;
    private string _mode = BedrockConnectorConfig.ModeChat;
    private string _systemPrompt = "";
    private int _maxTokens = BedrockConnectorConfig.DefaultMaxTokens;
    private double _temperature = BedrockConnectorConfig.DefaultTemperature;
    private double _topP = BedrockConnectorConfig.DefaultTopP;
    private string _inputField = BedrockConnectorConfig.DefaultInputField;
    private string _outputField = BedrockConnectorConfig.DefaultOutputField;
    private string _embeddingsField = BedrockConnectorConfig.DefaultEmbeddingsField;
    private bool _includeOriginal = BedrockConnectorConfig.DefaultIncludeOriginal;
    private string _outputFormat = BedrockConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private int _batchSize = BedrockConnectorConfig.DefaultBatchSize;
    private int _batchTimeoutMs = BedrockConnectorConfig.DefaultBatchTimeoutMs;
    private int _retryMax = BedrockConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = BedrockConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];
    private DateTime _lastFlush = DateTime.UtcNow;
    private HttpClient? _httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Read model config
        _modelId = config.TryGetValue(BedrockConnectorConfig.ModelIdConfig, out var modelId)
            ? modelId
            : BedrockConnectorConfig.DefaultModelId;

        // Read mode config
        _mode = config.TryGetValue(BedrockConnectorConfig.ModeConfig, out var mode)
            ? mode
            : BedrockConnectorConfig.ModeChat;

        // Read completion config
        _systemPrompt = config.TryGetValue(BedrockConnectorConfig.SystemPromptConfig, out var systemPrompt)
            ? systemPrompt
            : "";

        _maxTokens = config.TryGetValue(BedrockConnectorConfig.MaxTokensConfig, out var maxTokens) && int.TryParse(maxTokens, out var mt)
            ? mt
            : BedrockConnectorConfig.DefaultMaxTokens;

        _temperature = config.TryGetValue(BedrockConnectorConfig.TemperatureConfig, out var temp) && double.TryParse(temp, out var t)
            ? t
            : BedrockConnectorConfig.DefaultTemperature;

        _topP = config.TryGetValue(BedrockConnectorConfig.TopPConfig, out var topP) && double.TryParse(topP, out var tp)
            ? tp
            : BedrockConnectorConfig.DefaultTopP;

        // Read input/output config
        _inputField = config.TryGetValue(BedrockConnectorConfig.InputFieldConfig, out var inputField)
            ? inputField
            : BedrockConnectorConfig.DefaultInputField;

        _outputField = config.TryGetValue(BedrockConnectorConfig.OutputFieldConfig, out var outputField)
            ? outputField
            : BedrockConnectorConfig.DefaultOutputField;

        _embeddingsField = config.TryGetValue(BedrockConnectorConfig.EmbeddingsFieldConfig, out var embField)
            ? embField
            : BedrockConnectorConfig.DefaultEmbeddingsField;

        // Read batching config
        _batchSize = config.TryGetValue(BedrockConnectorConfig.BatchSizeConfig, out var batchSize) && int.TryParse(batchSize, out var bs)
            ? bs
            : BedrockConnectorConfig.DefaultBatchSize;

        _batchTimeoutMs = config.TryGetValue(BedrockConnectorConfig.BatchTimeoutMsConfig, out var batchTimeout) && int.TryParse(batchTimeout, out var bt)
            ? bt
            : BedrockConnectorConfig.DefaultBatchTimeoutMs;

        // Read retry config
        _retryMax = config.TryGetValue(BedrockConnectorConfig.RetryMaxConfig, out var retryMax) && int.TryParse(retryMax, out var rm)
            ? rm
            : BedrockConnectorConfig.DefaultRetryMax;

        _retryBackoffMs = config.TryGetValue(BedrockConnectorConfig.RetryBackoffMsConfig, out var retryBackoff) && int.TryParse(retryBackoff, out var rb)
            ? rb
            : BedrockConnectorConfig.DefaultRetryBackoffMs;

        // Read output config
        _includeOriginal = !config.TryGetValue(BedrockConnectorConfig.IncludeOriginalConfig, out var includeOriginal)
            || !bool.TryParse(includeOriginal, out var io)
            || io;

        _outputFormat = config.TryGetValue(BedrockConnectorConfig.OutputFormatConfig, out var outputFormat)
            ? outputFormat
            : BedrockConnectorConfig.FormatMerge;

        _webhookUrl = config.TryGetValue(BedrockConnectorConfig.WebhookUrlConfig, out var webhookUrl)
            ? webhookUrl
            : null;

        // Build AWS client configuration
        var region = config.TryGetValue(BedrockConnectorConfig.RegionConfig, out var r)
            ? RegionEndpoint.GetBySystemName(r)
            : RegionEndpoint.USEast1;

        AmazonBedrockRuntimeConfig clientConfig = new()
        {
            RegionEndpoint = region
        };

        // Handle custom endpoint (LocalStack)
        if (config.TryGetValue(BedrockConnectorConfig.EndpointConfig, out var endpoint) && !string.IsNullOrEmpty(endpoint))
        {
            clientConfig.ServiceURL = endpoint;
        }

        // Handle credentials
        if (config.TryGetValue(BedrockConnectorConfig.AccessKeyConfig, out var accessKey) && !string.IsNullOrEmpty(accessKey) &&
            config.TryGetValue(BedrockConnectorConfig.SecretKeyConfig, out var secretKey) && !string.IsNullOrEmpty(secretKey))
        {
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            _client = new AmazonBedrockRuntimeClient(credentials, clientConfig);
        }
        else
        {
            // Use default credentials chain (environment, profile, IAM role)
            _client = new AmazonBedrockRuntimeClient(clientConfig);
        }

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

        _client?.Dispose();
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

        // Process based on mode
        if (_mode == BedrockConnectorConfig.ModeEmbeddings)
        {
            await ProcessEmbeddingsAsync(recordsToProcess, cancellationToken);
        }
        else
        {
            await ProcessChatAsync(recordsToProcess, cancellationToken);
        }
    }

    private async Task ProcessChatAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        foreach (var record in records)
        {
            try
            {
                var inputText = ExtractInputText(record);
                if (string.IsNullOrEmpty(inputText)) continue;

                string? responseText = null;
                for (var attempt = 0; attempt <= _retryMax; attempt++)
                {
                    try
                    {
                        // Use the Converse API for unified chat interface
                        var converseRequest = new ConverseRequest
                        {
                            ModelId = _modelId,
                            InferenceConfig = new InferenceConfiguration
                            {
                                MaxTokens = _maxTokens,
                                Temperature = (float)_temperature,
                                TopP = (float)_topP
                            }
                        };

                        // Add system prompt if specified
                        if (!string.IsNullOrEmpty(_systemPrompt))
                        {
                            converseRequest.System = [new SystemContentBlock { Text = _systemPrompt }];
                        }

                        // Add user message
                        converseRequest.Messages =
                        [
                            new Message
                            {
                                Role = ConversationRole.User,
                                Content = [new ContentBlock { Text = inputText }]
                            }
                        ];

                        var response = await _client.ConverseAsync(converseRequest, cancellationToken);

                        // Extract response text
                        var outputMessage = response.Output?.Message;
                        if (outputMessage?.Content != null && outputMessage.Content.Count > 0)
                        {
                            responseText = outputMessage.Content[0].Text;
                        }
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (responseText == null) continue;

                var output = BuildOutput(record, responseText, null);
                await OutputResultAsync(output, cancellationToken);
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
            }
        }
    }

    private async Task ProcessEmbeddingsAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        foreach (var record in records)
        {
            try
            {
                var inputText = ExtractInputText(record);
                if (string.IsNullOrEmpty(inputText)) continue;

                float[]? embedding = null;
                for (var attempt = 0; attempt <= _retryMax; attempt++)
                {
                    try
                    {
                        // Use InvokeModel for embeddings (Titan, Cohere)
                        var requestBody = new JsonObject
                        {
                            ["inputText"] = inputText
                        };

                        var request = new InvokeModelRequest
                        {
                            ModelId = _modelId,
                            ContentType = "application/json",
                            Accept = "application/json",
                            Body = new MemoryStream(Encoding.UTF8.GetBytes(requestBody.ToJsonString()))
                        };

                        var response = await _client.InvokeModelAsync(request, cancellationToken);

                        // Parse response
                        using var reader = new StreamReader(response.Body);
                        var responseJson = await reader.ReadToEndAsync(cancellationToken);
                        var responseObj = JsonDocument.Parse(responseJson);

                        // Extract embedding (format varies by model)
                        if (responseObj.RootElement.TryGetProperty("embedding", out var embeddingProp))
                        {
                            embedding = embeddingProp.EnumerateArray()
                                .Select(e => e.GetSingle())
                                .ToArray();
                        }
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (embedding == null) continue;

                var output = BuildOutput(record, null, embedding);
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
            ? Encoding.UTF8.GetString(bytes)
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

    private JsonObject BuildOutput(SinkRecord record, string? responseText, float[]? embedding)
    {
        var output = new JsonObject();

        if (_includeOriginal && _outputFormat == BedrockConnectorConfig.FormatMerge)
        {
            // Try to parse original value and merge
            if (record.Value != null)
            {
                var valueStr = record.Value is byte[] bytes
                    ? Encoding.UTF8.GetString(bytes)
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

        // Add response or embedding
        if (responseText != null)
        {
            output[_outputField] = responseText;
        }

        if (embedding != null)
        {
            var embeddingArray = new JsonArray();
            foreach (var value in embedding)
            {
                embeddingArray.Add(value);
            }
            output[_embeddingsField] = embeddingArray;
        }

        // Add metadata
        output["_metadata"] = new JsonObject
        {
            ["topic"] = record.Topic,
            ["partition"] = record.Partition,
            ["offset"] = record.Offset,
            ["timestamp"] = record.Timestamp.ToString("O"),
            ["model"] = _modelId,
            ["mode"] = _mode
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
