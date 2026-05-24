namespace Kuestenlogik.Surgewave.Connector.HuggingFace;

using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that processes data using Hugging Face Inference API.
/// </summary>
public sealed class HuggingFaceSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions SerializerOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    private HttpClient? _client;
    private string _endpoint = HuggingFaceConnectorConfig.DefaultEndpoint;
    private string _modelId = "";
    private string _mode = HuggingFaceConnectorConfig.ModeSentiment;
    private string _inputField = HuggingFaceConnectorConfig.DefaultInputField;
    private string _outputField = HuggingFaceConnectorConfig.DefaultOutputField;
    private string _embeddingsField = HuggingFaceConnectorConfig.DefaultEmbeddingsField;
    private string _contextField = HuggingFaceConnectorConfig.DefaultContextField;
    private string _questionField = HuggingFaceConnectorConfig.DefaultQuestionField;
    private string[]? _candidateLabels;
    private bool _multiLabel = HuggingFaceConnectorConfig.DefaultMultiLabel;
    private int _maxNewTokens = HuggingFaceConnectorConfig.DefaultMaxNewTokens;
    private double _temperature = HuggingFaceConnectorConfig.DefaultTemperature;
    private int _topK = HuggingFaceConnectorConfig.DefaultTopK;
    private double _topP = HuggingFaceConnectorConfig.DefaultTopP;
    private bool _doSample = HuggingFaceConnectorConfig.DefaultDoSample;
    private bool _includeOriginal = HuggingFaceConnectorConfig.DefaultIncludeOriginal;
    private string _outputFormat = HuggingFaceConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private int _batchSize = HuggingFaceConnectorConfig.DefaultBatchSize;
    private int _batchTimeoutMs = HuggingFaceConnectorConfig.DefaultBatchTimeoutMs;
    private int _retryMax = HuggingFaceConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = HuggingFaceConnectorConfig.DefaultRetryBackoffMs;
    private bool _waitForModel = HuggingFaceConnectorConfig.DefaultWaitForModel;
    private readonly List<SinkRecord> _buffer = [];
    private DateTime _lastFlush = DateTime.UtcNow;
    private HttpClient? _webhookClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Read connection config
        _endpoint = config.TryGetValue(HuggingFaceConnectorConfig.EndpointConfig, out var endpoint) && !string.IsNullOrEmpty(endpoint)
            ? endpoint
            : HuggingFaceConnectorConfig.DefaultEndpoint;

        // Read mode config
        _mode = config.TryGetValue(HuggingFaceConnectorConfig.ModeConfig, out var mode)
            ? mode
            : HuggingFaceConnectorConfig.ModeSentiment;

        // Read model ID or use default based on mode
        _modelId = config.TryGetValue(HuggingFaceConnectorConfig.ModelIdConfig, out var modelId) && !string.IsNullOrEmpty(modelId)
            ? modelId
            : GetDefaultModelForMode(_mode);

        // Read input/output config
        _inputField = config.TryGetValue(HuggingFaceConnectorConfig.InputFieldConfig, out var inputField)
            ? inputField
            : HuggingFaceConnectorConfig.DefaultInputField;

        _outputField = config.TryGetValue(HuggingFaceConnectorConfig.OutputFieldConfig, out var outputField)
            ? outputField
            : HuggingFaceConnectorConfig.DefaultOutputField;

        _embeddingsField = config.TryGetValue(HuggingFaceConnectorConfig.EmbeddingsFieldConfig, out var embField)
            ? embField
            : HuggingFaceConnectorConfig.DefaultEmbeddingsField;

        _contextField = config.TryGetValue(HuggingFaceConnectorConfig.ContextFieldConfig, out var ctxField)
            ? ctxField
            : HuggingFaceConnectorConfig.DefaultContextField;

        _questionField = config.TryGetValue(HuggingFaceConnectorConfig.QuestionFieldConfig, out var qField)
            ? qField
            : HuggingFaceConnectorConfig.DefaultQuestionField;

        // Classification config
        if (config.TryGetValue(HuggingFaceConnectorConfig.CandidateLabelsConfig, out var labels) && !string.IsNullOrEmpty(labels))
        {
            _candidateLabels = labels.Split(',').Select(l => l.Trim()).ToArray();
        }

        _multiLabel = config.TryGetValue(HuggingFaceConnectorConfig.MultiLabelConfig, out var multiLabel) && bool.TryParse(multiLabel, out var ml) && ml;

        // Text generation config
        _maxNewTokens = config.TryGetValue(HuggingFaceConnectorConfig.MaxNewTokensConfig, out var maxTokens) && int.TryParse(maxTokens, out var mt)
            ? mt
            : HuggingFaceConnectorConfig.DefaultMaxNewTokens;

        _temperature = config.TryGetValue(HuggingFaceConnectorConfig.TemperatureConfig, out var temp) && double.TryParse(temp, out var t)
            ? t
            : HuggingFaceConnectorConfig.DefaultTemperature;

        _topK = config.TryGetValue(HuggingFaceConnectorConfig.TopKConfig, out var topK) && int.TryParse(topK, out var tk)
            ? tk
            : HuggingFaceConnectorConfig.DefaultTopK;

        _topP = config.TryGetValue(HuggingFaceConnectorConfig.TopPConfig, out var topP) && double.TryParse(topP, out var tp)
            ? tp
            : HuggingFaceConnectorConfig.DefaultTopP;

        _doSample = !config.TryGetValue(HuggingFaceConnectorConfig.DoSampleConfig, out var doSample) || !bool.TryParse(doSample, out var ds) || ds;

        // Read batching config
        _batchSize = config.TryGetValue(HuggingFaceConnectorConfig.BatchSizeConfig, out var batchSize) && int.TryParse(batchSize, out var bs)
            ? bs
            : HuggingFaceConnectorConfig.DefaultBatchSize;

        _batchTimeoutMs = config.TryGetValue(HuggingFaceConnectorConfig.BatchTimeoutMsConfig, out var batchTimeout) && int.TryParse(batchTimeout, out var bt)
            ? bt
            : HuggingFaceConnectorConfig.DefaultBatchTimeoutMs;

        // Read retry config
        _retryMax = config.TryGetValue(HuggingFaceConnectorConfig.RetryMaxConfig, out var retryMax) && int.TryParse(retryMax, out var rm)
            ? rm
            : HuggingFaceConnectorConfig.DefaultRetryMax;

        _retryBackoffMs = config.TryGetValue(HuggingFaceConnectorConfig.RetryBackoffMsConfig, out var retryBackoff) && int.TryParse(retryBackoff, out var rb)
            ? rb
            : HuggingFaceConnectorConfig.DefaultRetryBackoffMs;

        // Read output config
        _includeOriginal = !config.TryGetValue(HuggingFaceConnectorConfig.IncludeOriginalConfig, out var includeOriginal)
            || !bool.TryParse(includeOriginal, out var io)
            || io;

        _outputFormat = config.TryGetValue(HuggingFaceConnectorConfig.OutputFormatConfig, out var outputFormat)
            ? outputFormat
            : HuggingFaceConnectorConfig.FormatMerge;

        _webhookUrl = config.TryGetValue(HuggingFaceConnectorConfig.WebhookUrlConfig, out var webhookUrl) && !string.IsNullOrEmpty(webhookUrl)
            ? webhookUrl
            : null;

        _waitForModel = !config.TryGetValue(HuggingFaceConnectorConfig.WaitForModelConfig, out var waitForModel) || !bool.TryParse(waitForModel, out var wfm) || wfm;

        // Create HTTP client
        _client = new HttpClient();
        if (config.TryGetValue(HuggingFaceConnectorConfig.ApiKeyConfig, out var apiKey) && !string.IsNullOrEmpty(apiKey))
        {
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
        }

        if (!string.IsNullOrEmpty(_webhookUrl))
        {
            _webhookClient = new HttpClient();
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
        _webhookClient?.Dispose();
        _webhookClient = null;
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

        foreach (var record in recordsToProcess)
        {
            try
            {
                JsonNode? result = null;

                switch (_mode)
                {
                    case HuggingFaceConnectorConfig.ModeEmbeddings:
                        result = await ProcessEmbeddingsAsync(record, cancellationToken);
                        break;
                    case HuggingFaceConnectorConfig.ModeQuestionAnswering:
                        result = await ProcessQuestionAnsweringAsync(record, cancellationToken);
                        break;
                    case HuggingFaceConnectorConfig.ModeClassification:
                        result = await ProcessClassificationAsync(record, cancellationToken);
                        break;
                    case HuggingFaceConnectorConfig.ModeTextGeneration:
                        result = await ProcessTextGenerationAsync(record, cancellationToken);
                        break;
                    default:
                        result = await ProcessSimpleAsync(record, cancellationToken);
                        break;
                }

                if (result == null) continue;

                var output = BuildOutput(record, result);
                await OutputResultAsync(output, cancellationToken);
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
            }
        }
    }

    private async Task<JsonNode?> ProcessSimpleAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var inputText = ExtractInputText(record);
        if (string.IsNullOrEmpty(inputText)) return null;

        var url = $"{_endpoint}/{_modelId}";
        var payload = new { inputs = inputText };

        return await SendRequestAsync(url, payload, cancellationToken);
    }

    private async Task<JsonNode?> ProcessEmbeddingsAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var inputText = ExtractInputText(record);
        if (string.IsNullOrEmpty(inputText)) return null;

        var url = $"{_endpoint}/{_modelId}";
        var payload = new { inputs = inputText };

        return await SendRequestAsync(url, payload, cancellationToken);
    }

    private async Task<JsonNode?> ProcessQuestionAnsweringAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var (context, question) = ExtractQuestionAnsweringInputs(record);
        if (string.IsNullOrEmpty(context) || string.IsNullOrEmpty(question)) return null;

        var url = $"{_endpoint}/{_modelId}";
        var payload = new { inputs = new { context, question } };

        return await SendRequestAsync(url, payload, cancellationToken);
    }

    private async Task<JsonNode?> ProcessClassificationAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var inputText = ExtractInputText(record);
        if (string.IsNullOrEmpty(inputText)) return null;

        var url = $"{_endpoint}/{_modelId}";
        object payload;

        if (_candidateLabels != null && _candidateLabels.Length > 0)
        {
            // Zero-shot classification
            payload = new
            {
                inputs = inputText,
                parameters = new
                {
                    candidate_labels = _candidateLabels,
                    multi_label = _multiLabel
                }
            };
        }
        else
        {
            payload = new { inputs = inputText };
        }

        return await SendRequestAsync(url, payload, cancellationToken);
    }

    private async Task<JsonNode?> ProcessTextGenerationAsync(SinkRecord record, CancellationToken cancellationToken)
    {
        var inputText = ExtractInputText(record);
        if (string.IsNullOrEmpty(inputText)) return null;

        var url = $"{_endpoint}/{_modelId}";
        var payload = new
        {
            inputs = inputText,
            parameters = new
            {
                max_new_tokens = _maxNewTokens,
                temperature = _temperature,
                top_k = _topK,
                top_p = _topP,
                do_sample = _doSample
            }
        };

        return await SendRequestAsync(url, payload, cancellationToken);
    }

    private async Task<JsonNode?> SendRequestAsync(string url, object payload, CancellationToken cancellationToken)
    {
        if (_client == null) return null;

        var uri = new Uri(url);

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                using var content = new StringContent(JsonSerializer.Serialize(payload, SerializerOptions), Encoding.UTF8, "application/json");

                if (_waitForModel)
                {
                    content.Headers.Add("x-wait-for-model", "true");
                }

                var response = await _client.PostAsync(uri, content, cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    var responseText = await response.Content.ReadAsStringAsync(cancellationToken);
                    return JsonNode.Parse(responseText);
                }

                // Check if model is loading (503)
                if ((int)response.StatusCode == 503 && attempt < _retryMax)
                {
                    await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    continue;
                }

                // For other errors, throw
                response.EnsureSuccessStatusCode();
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }

        return null;
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
            return valueStr;
        }
        catch (JsonException)
        {
            return valueStr;
        }
    }

    private (string? context, string? question) ExtractQuestionAnsweringInputs(SinkRecord record)
    {
        if (record.Value == null) return (null, null);

        var valueStr = record.Value is byte[] bytes
            ? Encoding.UTF8.GetString(bytes)
            : record.Value.ToString();

        if (string.IsNullOrEmpty(valueStr)) return (null, null);

        try
        {
            var doc = JsonDocument.Parse(valueStr);
            string? context = null;
            string? question = null;

            if (doc.RootElement.TryGetProperty(_contextField, out var ctxField))
            {
                context = ctxField.GetString();
            }
            if (doc.RootElement.TryGetProperty(_questionField, out var qField))
            {
                question = qField.GetString();
            }

            return (context, question);
        }
        catch (JsonException)
        {
            return (null, null);
        }
    }

    private static string GetDefaultModelForMode(string mode)
    {
        return mode switch
        {
            HuggingFaceConnectorConfig.ModeSentiment => HuggingFaceConnectorConfig.DefaultSentimentModel,
            HuggingFaceConnectorConfig.ModeNer => HuggingFaceConnectorConfig.DefaultNerModel,
            HuggingFaceConnectorConfig.ModeClassification => HuggingFaceConnectorConfig.DefaultClassificationModel,
            HuggingFaceConnectorConfig.ModeEmbeddings => HuggingFaceConnectorConfig.DefaultEmbeddingsModel,
            HuggingFaceConnectorConfig.ModeTextGeneration => HuggingFaceConnectorConfig.DefaultTextGenerationModel,
            HuggingFaceConnectorConfig.ModeFillMask => HuggingFaceConnectorConfig.DefaultFillMaskModel,
            HuggingFaceConnectorConfig.ModeQuestionAnswering => HuggingFaceConnectorConfig.DefaultQuestionAnsweringModel,
            HuggingFaceConnectorConfig.ModeSummarization => HuggingFaceConnectorConfig.DefaultSummarizationModel,
            HuggingFaceConnectorConfig.ModeTranslation => HuggingFaceConnectorConfig.DefaultTranslationModel,
            _ => HuggingFaceConnectorConfig.DefaultSentimentModel
        };
    }

    private JsonObject BuildOutput(SinkRecord record, JsonNode result)
    {
        var output = new JsonObject();

        if (_includeOriginal && _outputFormat == HuggingFaceConnectorConfig.FormatMerge)
        {
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
                        output["original"] = valueStr;
                    }
                }
            }
        }

        // Add result based on mode
        if (_mode == HuggingFaceConnectorConfig.ModeEmbeddings && result is JsonArray embArray)
        {
            output[_embeddingsField] = embArray.DeepClone();
        }
        else
        {
            output[_outputField] = result.DeepClone();
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

        if (!string.IsNullOrEmpty(_webhookUrl) && _webhookClient != null)
        {
            await _webhookClient.PostAsJsonAsync(_webhookUrl, output, cancellationToken);
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
            _webhookClient?.Dispose();
            _webhookClient = null;
        }
        base.Dispose(disposing);
    }
}
