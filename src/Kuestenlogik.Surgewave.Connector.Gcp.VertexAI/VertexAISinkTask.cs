namespace Kuestenlogik.Surgewave.Connector.Gcp.VertexAI;

using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.AIPlatform.V1;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that processes records through GCP Vertex AI (Gemini models).
/// Supports chat completions and embeddings generation.
/// </summary>
public sealed class VertexAISinkTask : SinkTask
{
    private PredictionServiceClient? _predictionClient;
    private string _projectId = "";
    private string _location = VertexAIConnectorConfig.DefaultLocation;
    private string _mode = VertexAIConnectorConfig.ModeCompletions;
    private string _model = VertexAIConnectorConfig.DefaultModel;
    private string _embeddingsModel = VertexAIConnectorConfig.DefaultEmbeddingsModel;
    private string _systemPrompt = "";
    private int _maxTokens = VertexAIConnectorConfig.DefaultMaxTokens;
    private double _temperature = VertexAIConnectorConfig.DefaultTemperature;
    private double _topP = VertexAIConnectorConfig.DefaultTopP;
    private int _topK = VertexAIConnectorConfig.DefaultTopK;
    private int _embeddingsDimensions = VertexAIConnectorConfig.DefaultEmbeddingsDimensions;
    private string _inputField = VertexAIConnectorConfig.DefaultInputField;
    private string _outputField = VertexAIConnectorConfig.DefaultOutputField;
    private string _embeddingsField = VertexAIConnectorConfig.DefaultEmbeddingsField;
    private bool _includeOriginal = VertexAIConnectorConfig.DefaultIncludeOriginal;
    private string _outputFormat = VertexAIConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private int _batchSize = VertexAIConnectorConfig.DefaultBatchSize;
    private int _batchTimeoutMs = VertexAIConnectorConfig.DefaultBatchTimeoutMs;
    private int _retryMax = VertexAIConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = VertexAIConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];
    private DateTime _lastFlush = DateTime.UtcNow;
    private HttpClient? _httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Get project ID from config or environment
        _projectId = config.TryGetValue(VertexAIConnectorConfig.ProjectIdConfig, out var proj) && !string.IsNullOrEmpty(proj)
            ? proj
            : Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT") ?? "";

        if (string.IsNullOrEmpty(_projectId))
            throw new ArgumentException($"Missing required config: {VertexAIConnectorConfig.ProjectIdConfig}");

        // Get location
        _location = config.TryGetValue(VertexAIConnectorConfig.LocationConfig, out var loc) && !string.IsNullOrEmpty(loc)
            ? loc
            : VertexAIConnectorConfig.DefaultLocation;

        // Read mode config
        _mode = config.TryGetValue(VertexAIConnectorConfig.ModeConfig, out var mode)
            ? mode
            : VertexAIConnectorConfig.ModeCompletions;

        // Read model config
        _model = config.TryGetValue(VertexAIConnectorConfig.ModelConfig, out var model)
            ? model
            : VertexAIConnectorConfig.DefaultModel;

        _embeddingsModel = config.TryGetValue(VertexAIConnectorConfig.EmbeddingsModelConfig, out var embModel)
            ? embModel
            : VertexAIConnectorConfig.DefaultEmbeddingsModel;

        // Read completions config
        _systemPrompt = config.TryGetValue(VertexAIConnectorConfig.SystemPromptConfig, out var systemPrompt)
            ? systemPrompt
            : "";

        _maxTokens = config.TryGetValue(VertexAIConnectorConfig.MaxTokensConfig, out var maxTokens) && int.TryParse(maxTokens, out var mt)
            ? mt
            : VertexAIConnectorConfig.DefaultMaxTokens;

        _temperature = config.TryGetValue(VertexAIConnectorConfig.TemperatureConfig, out var temp) && double.TryParse(temp, out var t)
            ? t
            : VertexAIConnectorConfig.DefaultTemperature;

        _topP = config.TryGetValue(VertexAIConnectorConfig.TopPConfig, out var topP) && double.TryParse(topP, out var tp)
            ? tp
            : VertexAIConnectorConfig.DefaultTopP;

        _topK = config.TryGetValue(VertexAIConnectorConfig.TopKConfig, out var topK) && int.TryParse(topK, out var tk)
            ? tk
            : VertexAIConnectorConfig.DefaultTopK;

        _embeddingsDimensions = config.TryGetValue(VertexAIConnectorConfig.EmbeddingsDimensionsConfig, out var dims) && int.TryParse(dims, out var d)
            ? d
            : VertexAIConnectorConfig.DefaultEmbeddingsDimensions;

        // Read input/output config
        _inputField = config.TryGetValue(VertexAIConnectorConfig.InputFieldConfig, out var inputField)
            ? inputField
            : VertexAIConnectorConfig.DefaultInputField;

        _outputField = config.TryGetValue(VertexAIConnectorConfig.OutputFieldConfig, out var outputField)
            ? outputField
            : VertexAIConnectorConfig.DefaultOutputField;

        _embeddingsField = config.TryGetValue(VertexAIConnectorConfig.EmbeddingsFieldConfig, out var embField)
            ? embField
            : VertexAIConnectorConfig.DefaultEmbeddingsField;

        // Read batching config
        _batchSize = config.TryGetValue(VertexAIConnectorConfig.BatchSizeConfig, out var batchSize) && int.TryParse(batchSize, out var bs)
            ? bs
            : VertexAIConnectorConfig.DefaultBatchSize;

        _batchTimeoutMs = config.TryGetValue(VertexAIConnectorConfig.BatchTimeoutMsConfig, out var batchTimeout) && int.TryParse(batchTimeout, out var bt)
            ? bt
            : VertexAIConnectorConfig.DefaultBatchTimeoutMs;

        // Read retry config
        _retryMax = config.TryGetValue(VertexAIConnectorConfig.RetryMaxConfig, out var retryMax) && int.TryParse(retryMax, out var rm)
            ? rm
            : VertexAIConnectorConfig.DefaultRetryMax;

        _retryBackoffMs = config.TryGetValue(VertexAIConnectorConfig.RetryBackoffMsConfig, out var retryBackoff) && int.TryParse(retryBackoff, out var rb)
            ? rb
            : VertexAIConnectorConfig.DefaultRetryBackoffMs;

        // Read output config
        _includeOriginal = !config.TryGetValue(VertexAIConnectorConfig.IncludeOriginalConfig, out var includeOriginal)
            || !bool.TryParse(includeOriginal, out var io)
            || io;

        _outputFormat = config.TryGetValue(VertexAIConnectorConfig.OutputFormatConfig, out var outputFormat)
            ? outputFormat
            : VertexAIConnectorConfig.FormatMerge;

        _webhookUrl = config.TryGetValue(VertexAIConnectorConfig.WebhookUrlConfig, out var webhookUrl)
            ? webhookUrl
            : null;

        // Build client with location-specific endpoint
        var endpoint = $"{_location}-aiplatform.googleapis.com";
        var builder = new PredictionServiceClientBuilder
        {
            Endpoint = endpoint
        };

        // Handle credentials
#pragma warning disable CS0618 // GoogleCredential.FromJson/FromFile - CredentialFactory alternative requires internal IGoogleCredential
        if (config.TryGetValue(VertexAIConnectorConfig.CredentialsJsonConfig, out var credJson) && !string.IsNullOrEmpty(credJson))
        {
            builder.GoogleCredential = GoogleCredential.FromJson(credJson);
        }
        else if (config.TryGetValue(VertexAIConnectorConfig.CredentialsPathConfig, out var credPath) && !string.IsNullOrEmpty(credPath))
        {
            builder.GoogleCredential = GoogleCredential.FromFile(credPath);
        }
#pragma warning restore CS0618
        // Otherwise uses Application Default Credentials (ADC)

        _predictionClient = builder.Build();

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

        _predictionClient = null;
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
        if (_buffer.Count == 0 || _predictionClient == null) return;

        var recordsToProcess = _buffer.ToList();
        _buffer.Clear();
        _lastFlush = DateTime.UtcNow;

        // Process based on mode
        if (_mode == VertexAIConnectorConfig.ModeEmbeddings)
        {
            await ProcessEmbeddingsAsync(recordsToProcess, cancellationToken);
        }
        else
        {
            await ProcessCompletionsAsync(recordsToProcess, cancellationToken);
        }
    }

    private async Task ProcessCompletionsAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_predictionClient == null) return;

        // Build model endpoint
        var modelEndpoint = $"projects/{_projectId}/locations/{_location}/publishers/google/models/{_model}";

        foreach (var record in records)
        {
            try
            {
                // Extract input text from record
                var inputText = ExtractInputText(record);
                if (string.IsNullOrEmpty(inputText)) continue;

                // Create generate content request with retry
                string? responseText = null;
                for (var attempt = 0; attempt <= _retryMax; attempt++)
                {
                    try
                    {
                        var request = new GenerateContentRequest
                        {
                            Model = modelEndpoint,
                            GenerationConfig = new GenerationConfig
                            {
                                MaxOutputTokens = _maxTokens,
                                Temperature = (float)_temperature,
                                TopP = (float)_topP,
                                TopK = _topK
                            }
                        };

                        // Add system instruction if specified
                        if (!string.IsNullOrEmpty(_systemPrompt))
                        {
                            request.SystemInstruction = new Content
                            {
                                Parts = { new Part { Text = _systemPrompt } }
                            };
                        }

                        // Add user message
                        request.Contents.Add(new Content
                        {
                            Role = "user",
                            Parts = { new Part { Text = inputText } }
                        });

                        var response = await _predictionClient.GenerateContentAsync(request, cancellationToken);

                        // Extract text from response
                        responseText = response.Candidates.FirstOrDefault()?.Content?.Parts?.FirstOrDefault()?.Text;
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (responseText == null) continue;

                // Build output
                var output = BuildOutput(record, responseText, null);

                // Send to webhook or log
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
        if (_predictionClient == null) return;

        // Build model endpoint for embeddings
        var modelEndpoint = $"projects/{_projectId}/locations/{_location}/publishers/google/models/{_embeddingsModel}";

        foreach (var record in records)
        {
            try
            {
                // Extract input text from record
                var inputText = ExtractInputText(record);
                if (string.IsNullOrEmpty(inputText)) continue;

                // Create embed request with retry
                float[]? embedding = null;
                for (var attempt = 0; attempt <= _retryMax; attempt++)
                {
                    try
                    {
                        // For text embeddings, we use the predict endpoint with embedding instances
                        var endpoint = EndpointName.FromProjectLocationPublisherModel(
                            _projectId, _location, "google", _embeddingsModel);

                        var instance = Google.Protobuf.WellKnownTypes.Value.ForStruct(
                            new Google.Protobuf.WellKnownTypes.Struct
                            {
                                Fields =
                                {
                                    ["content"] = Google.Protobuf.WellKnownTypes.Value.ForString(inputText)
                                }
                            });

                        var parameters = Google.Protobuf.WellKnownTypes.Value.ForStruct(
                            new Google.Protobuf.WellKnownTypes.Struct
                            {
                                Fields =
                                {
                                    ["outputDimensionality"] = Google.Protobuf.WellKnownTypes.Value.ForNumber(_embeddingsDimensions)
                                }
                            });

                        var response = await _predictionClient.PredictAsync(
                            endpoint,
                            [instance],
                            parameters,
                            cancellationToken);

                        // Extract embedding from response
                        var prediction = response.Predictions.FirstOrDefault();
                        if (prediction != null && prediction.StructValue != null)
                        {
                            if (prediction.StructValue.Fields.TryGetValue("embeddings", out var embeddingsValue) &&
                                embeddingsValue.StructValue != null &&
                                embeddingsValue.StructValue.Fields.TryGetValue("values", out var valuesValue))
                            {
                                embedding = valuesValue.ListValue.Values
                                    .Select(v => (float)v.NumberValue)
                                    .ToArray();
                            }
                        }
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (embedding == null) continue;

                // Build output
                var output = BuildOutput(record, null, embedding);

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

    private JsonObject BuildOutput(SinkRecord record, string? responseText, float[]? embedding)
    {
        var output = new JsonObject();

        if (_includeOriginal && _outputFormat == VertexAIConnectorConfig.FormatMerge)
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
            ["model"] = _mode == VertexAIConnectorConfig.ModeEmbeddings ? _embeddingsModel : _model,
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
            _httpClient?.Dispose();
            _httpClient = null;
        }
        base.Dispose(disposing);
    }
}
