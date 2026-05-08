namespace Kuestenlogik.Surgewave.Connector.Gcp.Language;

using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Google.Cloud.Language.V1;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that analyzes text using Google Cloud Natural Language API.
/// </summary>
public sealed class LanguageSinkTask : SinkTask
{
    private LanguageServiceClient? _client;
    private string _mode = LanguageConnectorConfig.ModeSentiment;
    private string _language = LanguageConnectorConfig.DefaultLanguage;
    private string _inputField = LanguageConnectorConfig.DefaultInputField;
    private string _outputField = LanguageConnectorConfig.DefaultOutputField;
    private bool _includeOriginal = LanguageConnectorConfig.DefaultIncludeOriginal;
    private string _outputFormat = LanguageConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private int _batchSize = LanguageConnectorConfig.DefaultBatchSize;
    private int _batchTimeoutMs = LanguageConnectorConfig.DefaultBatchTimeoutMs;
    private int _retryMax = LanguageConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = LanguageConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];
    private DateTime _lastFlush = DateTime.UtcNow;
    private HttpClient? _httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Read mode config
        _mode = config.TryGetValue(LanguageConnectorConfig.ModeConfig, out var mode)
            ? mode
            : LanguageConnectorConfig.ModeSentiment;

        _language = config.TryGetValue(LanguageConnectorConfig.LanguageConfig, out var lang)
            ? lang
            : LanguageConnectorConfig.DefaultLanguage;

        // Read input/output config
        _inputField = config.TryGetValue(LanguageConnectorConfig.InputFieldConfig, out var inputField)
            ? inputField
            : LanguageConnectorConfig.DefaultInputField;

        _outputField = config.TryGetValue(LanguageConnectorConfig.OutputFieldConfig, out var outputField)
            ? outputField
            : LanguageConnectorConfig.DefaultOutputField;

        // Read batching config
        _batchSize = config.TryGetValue(LanguageConnectorConfig.BatchSizeConfig, out var batchSize) && int.TryParse(batchSize, out var bs)
            ? bs
            : LanguageConnectorConfig.DefaultBatchSize;

        _batchTimeoutMs = config.TryGetValue(LanguageConnectorConfig.BatchTimeoutMsConfig, out var batchTimeout) && int.TryParse(batchTimeout, out var bt)
            ? bt
            : LanguageConnectorConfig.DefaultBatchTimeoutMs;

        // Read retry config
        _retryMax = config.TryGetValue(LanguageConnectorConfig.RetryMaxConfig, out var retryMax) && int.TryParse(retryMax, out var rm)
            ? rm
            : LanguageConnectorConfig.DefaultRetryMax;

        _retryBackoffMs = config.TryGetValue(LanguageConnectorConfig.RetryBackoffMsConfig, out var retryBackoff) && int.TryParse(retryBackoff, out var rb)
            ? rb
            : LanguageConnectorConfig.DefaultRetryBackoffMs;

        // Read output config
        _includeOriginal = !config.TryGetValue(LanguageConnectorConfig.IncludeOriginalConfig, out var includeOriginal)
            || !bool.TryParse(includeOriginal, out var io)
            || io;

        _outputFormat = config.TryGetValue(LanguageConnectorConfig.OutputFormatConfig, out var outputFormat)
            ? outputFormat
            : LanguageConnectorConfig.FormatMerge;

        _webhookUrl = config.TryGetValue(LanguageConnectorConfig.WebhookUrlConfig, out var webhookUrl)
            ? webhookUrl
            : null;

        // Build client
        var builder = new LanguageServiceClientBuilder();

        // Handle credentials
        if (config.TryGetValue(LanguageConnectorConfig.CredentialsJsonConfig, out var credJson) && !string.IsNullOrEmpty(credJson))
        {
            builder.JsonCredentials = credJson;
        }
        else if (config.TryGetValue(LanguageConnectorConfig.CredentialsPathConfig, out var credPath) && !string.IsNullOrEmpty(credPath))
        {
            builder.CredentialsPath = credPath;
        }
        // Otherwise uses Application Default Credentials (ADC)

        _client = builder.Build();

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

        foreach (var record in recordsToProcess)
        {
            try
            {
                // Extract input text from record
                var inputText = ExtractInputText(record);
                if (string.IsNullOrEmpty(inputText)) continue;

                // Analyze text with retry
                JsonObject? analysisResult = null;
                for (var attempt = 0; attempt <= _retryMax; attempt++)
                {
                    try
                    {
                        analysisResult = await AnalyzeTextAsync(inputText, cancellationToken);
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (analysisResult == null) continue;

                // Build output
                var output = BuildOutput(record, analysisResult);

                // Send to webhook or log
                await OutputResultAsync(output, cancellationToken);
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
            }
        }
    }

    private async Task<JsonObject> AnalyzeTextAsync(string text, CancellationToken cancellationToken)
    {
        if (_client == null) throw new InvalidOperationException("Client not initialized");

        var document = new Document
        {
            Content = text,
            Type = Document.Types.Type.PlainText,
            Language = _language
        };

        var result = new JsonObject();

        switch (_mode)
        {
            case LanguageConnectorConfig.ModeSentiment:
                var sentimentResponse = await _client.AnalyzeSentimentAsync(document, cancellationToken: cancellationToken);
                result["sentiment"] = new JsonObject
                {
                    ["score"] = sentimentResponse.DocumentSentiment.Score,
                    ["magnitude"] = sentimentResponse.DocumentSentiment.Magnitude
                };
                break;

            case LanguageConnectorConfig.ModeEntities:
                var entitiesResponse = await _client.AnalyzeEntitiesAsync(document, cancellationToken: cancellationToken);
                var entitiesArray = new JsonArray();
                foreach (var entity in entitiesResponse.Entities)
                {
                    entitiesArray.Add(new JsonObject
                    {
                        ["name"] = entity.Name,
                        ["type"] = entity.Type.ToString(),
                        ["salience"] = entity.Salience
                    });
                }
                result["entities"] = entitiesArray;
                break;

            case LanguageConnectorConfig.ModeSyntax:
                var syntaxResponse = await _client.AnalyzeSentimentAsync(document, cancellationToken: cancellationToken);
                var tokensArray = new JsonArray();
                foreach (var sentence in syntaxResponse.Sentences)
                {
                    tokensArray.Add(new JsonObject
                    {
                        ["text"] = sentence.Text.Content,
                        ["beginOffset"] = sentence.Text.BeginOffset,
                        ["sentiment"] = new JsonObject
                        {
                            ["score"] = sentence.Sentiment.Score,
                            ["magnitude"] = sentence.Sentiment.Magnitude
                        }
                    });
                }
                result["sentences"] = tokensArray;
                break;

            case LanguageConnectorConfig.ModeClassify:
                var classifyResponse = await _client.ClassifyTextAsync(document, cancellationToken: cancellationToken);
                var categoriesArray = new JsonArray();
                foreach (var category in classifyResponse.Categories)
                {
                    categoriesArray.Add(new JsonObject
                    {
                        ["name"] = category.Name,
                        ["confidence"] = category.Confidence
                    });
                }
                result["categories"] = categoriesArray;
                break;

            case LanguageConnectorConfig.ModeAll:
                // Run all analyses
                var allSentiment = await _client.AnalyzeSentimentAsync(document, cancellationToken: cancellationToken);
                result["sentiment"] = new JsonObject
                {
                    ["score"] = allSentiment.DocumentSentiment.Score,
                    ["magnitude"] = allSentiment.DocumentSentiment.Magnitude
                };

                var allEntities = await _client.AnalyzeEntitiesAsync(document, cancellationToken: cancellationToken);
                var allEntitiesArray = new JsonArray();
                foreach (var entity in allEntities.Entities)
                {
                    allEntitiesArray.Add(new JsonObject
                    {
                        ["name"] = entity.Name,
                        ["type"] = entity.Type.ToString(),
                        ["salience"] = entity.Salience
                    });
                }
                result["entities"] = allEntitiesArray;

                try
                {
                    var allClassify = await _client.ClassifyTextAsync(document, cancellationToken: cancellationToken);
                    var allCategoriesArray = new JsonArray();
                    foreach (var category in allClassify.Categories)
                    {
                        allCategoriesArray.Add(new JsonObject
                        {
                            ["name"] = category.Name,
                            ["confidence"] = category.Confidence
                        });
                    }
                    result["categories"] = allCategoriesArray;
                }
                catch
                {
                    // Classification may fail for short texts
                    result["categories"] = new JsonArray();
                }
                break;
        }

        return result;
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

    private JsonObject BuildOutput(SinkRecord record, JsonObject analysis)
    {
        var output = new JsonObject();

        if (_includeOriginal && _outputFormat == LanguageConnectorConfig.FormatMerge)
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

        // Add analysis result
        output[_outputField] = analysis;

        // Add metadata
        output["_metadata"] = new JsonObject
        {
            ["topic"] = record.Topic,
            ["partition"] = record.Partition,
            ["offset"] = record.Offset,
            ["timestamp"] = record.Timestamp.ToString("O"),
            ["mode"] = _mode,
            ["language"] = _language
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
