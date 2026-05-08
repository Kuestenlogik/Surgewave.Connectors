namespace Kuestenlogik.Surgewave.Connector.Aws.Comprehend;

using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Nodes;
using Amazon;
using Amazon.Comprehend;
using Amazon.Comprehend.Model;
using Amazon.Runtime;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that analyzes text using Amazon Comprehend.
/// </summary>
public sealed class ComprehendSinkTask : SinkTask
{
    private AmazonComprehendClient? _client;
    private string _mode = ComprehendConnectorConfig.ModeSentiment;
    private string _language = ComprehendConnectorConfig.DefaultLanguage;
    private string _inputField = ComprehendConnectorConfig.DefaultInputField;
    private string _outputField = ComprehendConnectorConfig.DefaultOutputField;
    private bool _includeOriginal = ComprehendConnectorConfig.DefaultIncludeOriginal;
    private string _outputFormat = ComprehendConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private int _batchSize = ComprehendConnectorConfig.DefaultBatchSize;
    private int _batchTimeoutMs = ComprehendConnectorConfig.DefaultBatchTimeoutMs;
    private int _retryMax = ComprehendConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = ComprehendConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];
    private DateTime _lastFlush = DateTime.UtcNow;
    private HttpClient? _httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Read mode config
        _mode = config.TryGetValue(ComprehendConnectorConfig.ModeConfig, out var mode)
            ? mode
            : ComprehendConnectorConfig.ModeSentiment;

        _language = config.TryGetValue(ComprehendConnectorConfig.LanguageConfig, out var lang)
            ? lang
            : ComprehendConnectorConfig.DefaultLanguage;

        // Read input/output config
        _inputField = config.TryGetValue(ComprehendConnectorConfig.InputFieldConfig, out var inputField)
            ? inputField
            : ComprehendConnectorConfig.DefaultInputField;

        _outputField = config.TryGetValue(ComprehendConnectorConfig.OutputFieldConfig, out var outputField)
            ? outputField
            : ComprehendConnectorConfig.DefaultOutputField;

        // Read batching config
        _batchSize = config.TryGetValue(ComprehendConnectorConfig.BatchSizeConfig, out var batchSize) && int.TryParse(batchSize, out var bs)
            ? Math.Min(bs, 25) // AWS Comprehend batch limit
            : ComprehendConnectorConfig.DefaultBatchSize;

        _batchTimeoutMs = config.TryGetValue(ComprehendConnectorConfig.BatchTimeoutMsConfig, out var batchTimeout) && int.TryParse(batchTimeout, out var bt)
            ? bt
            : ComprehendConnectorConfig.DefaultBatchTimeoutMs;

        // Read retry config
        _retryMax = config.TryGetValue(ComprehendConnectorConfig.RetryMaxConfig, out var retryMax) && int.TryParse(retryMax, out var rm)
            ? rm
            : ComprehendConnectorConfig.DefaultRetryMax;

        _retryBackoffMs = config.TryGetValue(ComprehendConnectorConfig.RetryBackoffMsConfig, out var retryBackoff) && int.TryParse(retryBackoff, out var rb)
            ? rb
            : ComprehendConnectorConfig.DefaultRetryBackoffMs;

        // Read output config
        _includeOriginal = !config.TryGetValue(ComprehendConnectorConfig.IncludeOriginalConfig, out var includeOriginal)
            || !bool.TryParse(includeOriginal, out var io)
            || io;

        _outputFormat = config.TryGetValue(ComprehendConnectorConfig.OutputFormatConfig, out var outputFormat)
            ? outputFormat
            : ComprehendConnectorConfig.FormatMerge;

        _webhookUrl = config.TryGetValue(ComprehendConnectorConfig.WebhookUrlConfig, out var webhookUrl)
            ? webhookUrl
            : null;

        // Build AWS client configuration
        var region = config.TryGetValue(ComprehendConnectorConfig.RegionConfig, out var r)
            ? RegionEndpoint.GetBySystemName(r)
            : RegionEndpoint.USEast1;

        AmazonComprehendConfig clientConfig = new()
        {
            RegionEndpoint = region
        };

        // Handle custom endpoint (LocalStack)
        if (config.TryGetValue(ComprehendConnectorConfig.EndpointConfig, out var endpoint) && !string.IsNullOrEmpty(endpoint))
        {
            clientConfig.ServiceURL = endpoint;
        }

        // Handle credentials
        if (config.TryGetValue(ComprehendConnectorConfig.AccessKeyConfig, out var accessKey) && !string.IsNullOrEmpty(accessKey) &&
            config.TryGetValue(ComprehendConnectorConfig.SecretKeyConfig, out var secretKey) && !string.IsNullOrEmpty(secretKey))
        {
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            _client = new AmazonComprehendClient(credentials, clientConfig);
        }
        else
        {
            // Use default credentials chain (environment, profile, IAM role)
            _client = new AmazonComprehendClient(clientConfig);
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

        // Process records based on mode
        if (_mode == ComprehendConnectorConfig.ModeLanguage)
        {
            // Language detection doesn't require pre-specified language
            await ProcessLanguageDetectionAsync(recordsToProcess, cancellationToken);
        }
        else
        {
            // Other modes process with batch APIs
            await ProcessBatchAsync(recordsToProcess, cancellationToken);
        }
    }

    private async Task ProcessLanguageDetectionAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var texts = new List<string>();
        var recordMap = new Dictionary<int, SinkRecord>();

        foreach (var record in records)
        {
            var text = ExtractInputText(record);
            if (!string.IsNullOrEmpty(text))
            {
                recordMap[texts.Count] = record;
                texts.Add(text);
            }
        }

        if (texts.Count == 0) return;

        try
        {
            var request = new BatchDetectDominantLanguageRequest
            {
                TextList = texts
            };

            BatchDetectDominantLanguageResponse? response = null;
            for (var attempt = 0; attempt <= _retryMax; attempt++)
            {
                try
                {
                    response = await _client.BatchDetectDominantLanguageAsync(request, cancellationToken);
                    break;
                }
                catch (Exception) when (attempt < _retryMax)
                {
                    await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                }
            }

            if (response == null) return;

            for (var i = 0; i < response.ResultList.Count; i++)
            {
                var result = response.ResultList[i];
                if (!recordMap.TryGetValue(result.Index.GetValueOrDefault(), out var record)) continue;

                var analysis = new JsonObject();
                var languagesArray = new JsonArray();
                foreach (var lang in result.Languages)
                {
                    languagesArray.Add(new JsonObject
                    {
                        ["code"] = lang.LanguageCode,
                        ["score"] = lang.Score
                    });
                }
                analysis["languages"] = languagesArray;

                var output = BuildOutput(record, analysis);
                await OutputResultAsync(output, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            Context.RaiseError?.Invoke(ex);
        }
    }

    private async Task ProcessBatchAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var texts = new List<string>();
        var recordMap = new Dictionary<int, SinkRecord>();

        foreach (var record in records)
        {
            var text = ExtractInputText(record);
            if (!string.IsNullOrEmpty(text))
            {
                recordMap[texts.Count] = record;
                texts.Add(text);
            }
        }

        if (texts.Count == 0) return;

        var results = new Dictionary<int, JsonObject>();

        // Initialize result objects
        foreach (var idx in recordMap.Keys)
        {
            results[idx] = new JsonObject();
        }

        try
        {
            // Run analyses based on mode
            if (_mode == ComprehendConnectorConfig.ModeSentiment || _mode == ComprehendConnectorConfig.ModeAll)
            {
                await ProcessSentimentAsync(texts, results, cancellationToken);
            }

            if (_mode == ComprehendConnectorConfig.ModeEntities || _mode == ComprehendConnectorConfig.ModeAll)
            {
                await ProcessEntitiesAsync(texts, results, cancellationToken);
            }

            if (_mode == ComprehendConnectorConfig.ModeKeyPhrases || _mode == ComprehendConnectorConfig.ModeAll)
            {
                await ProcessKeyPhrasesAsync(texts, results, cancellationToken);
            }

            if (_mode == ComprehendConnectorConfig.ModePii || _mode == ComprehendConnectorConfig.ModeAll)
            {
                await ProcessPiiAsync(texts, results, cancellationToken);
            }

            if (_mode == ComprehendConnectorConfig.ModeSyntax || _mode == ComprehendConnectorConfig.ModeAll)
            {
                await ProcessSyntaxAsync(texts, results, cancellationToken);
            }

            // Output results
            foreach (var kvp in recordMap)
            {
                if (results.TryGetValue(kvp.Key, out var analysis))
                {
                    var output = BuildOutput(kvp.Value, analysis);
                    await OutputResultAsync(output, cancellationToken);
                }
            }
        }
        catch (Exception ex)
        {
            Context.RaiseError?.Invoke(ex);
        }
    }

    private async Task ProcessSentimentAsync(List<string> texts, Dictionary<int, JsonObject> results, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var request = new BatchDetectSentimentRequest
        {
            TextList = texts,
            LanguageCode = _language
        };

        BatchDetectSentimentResponse? response = null;
        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                response = await _client.BatchDetectSentimentAsync(request, cancellationToken);
                break;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }

        if (response == null) return;

        foreach (var result in response.ResultList)
        {
            if (results.TryGetValue(result.Index.GetValueOrDefault(), out var analysis))
            {
                analysis["sentiment"] = new JsonObject
                {
                    ["sentiment"] = result.Sentiment.Value,
                    ["positive"] = result.SentimentScore.Positive,
                    ["negative"] = result.SentimentScore.Negative,
                    ["neutral"] = result.SentimentScore.Neutral,
                    ["mixed"] = result.SentimentScore.Mixed
                };
            }
        }
    }

    private async Task ProcessEntitiesAsync(List<string> texts, Dictionary<int, JsonObject> results, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var request = new BatchDetectEntitiesRequest
        {
            TextList = texts,
            LanguageCode = _language
        };

        BatchDetectEntitiesResponse? response = null;
        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                response = await _client.BatchDetectEntitiesAsync(request, cancellationToken);
                break;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }

        if (response == null) return;

        foreach (var result in response.ResultList)
        {
            if (results.TryGetValue(result.Index.GetValueOrDefault(), out var analysis))
            {
                var entitiesArray = new JsonArray();
                foreach (var entity in result.Entities)
                {
                    entitiesArray.Add(new JsonObject
                    {
                        ["text"] = entity.Text,
                        ["type"] = entity.Type.Value,
                        ["score"] = entity.Score,
                        ["beginOffset"] = entity.BeginOffset,
                        ["endOffset"] = entity.EndOffset
                    });
                }
                analysis["entities"] = entitiesArray;
            }
        }
    }

    private async Task ProcessKeyPhrasesAsync(List<string> texts, Dictionary<int, JsonObject> results, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var request = new BatchDetectKeyPhrasesRequest
        {
            TextList = texts,
            LanguageCode = _language
        };

        BatchDetectKeyPhrasesResponse? response = null;
        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                response = await _client.BatchDetectKeyPhrasesAsync(request, cancellationToken);
                break;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }

        if (response == null) return;

        foreach (var result in response.ResultList)
        {
            if (results.TryGetValue(result.Index.GetValueOrDefault(), out var analysis))
            {
                var keyPhrasesArray = new JsonArray();
                foreach (var phrase in result.KeyPhrases)
                {
                    keyPhrasesArray.Add(new JsonObject
                    {
                        ["text"] = phrase.Text,
                        ["score"] = phrase.Score,
                        ["beginOffset"] = phrase.BeginOffset,
                        ["endOffset"] = phrase.EndOffset
                    });
                }
                analysis["keyPhrases"] = keyPhrasesArray;
            }
        }
    }

    private async Task ProcessPiiAsync(List<string> texts, Dictionary<int, JsonObject> results, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        // PII detection doesn't have batch API, process one at a time
        for (var i = 0; i < texts.Count; i++)
        {
            if (!results.TryGetValue(i, out var analysis)) continue;

            var request = new DetectPiiEntitiesRequest
            {
                Text = texts[i],
                LanguageCode = _language
            };

            DetectPiiEntitiesResponse? response = null;
            for (var attempt = 0; attempt <= _retryMax; attempt++)
            {
                try
                {
                    response = await _client.DetectPiiEntitiesAsync(request, cancellationToken);
                    break;
                }
                catch (Exception) when (attempt < _retryMax)
                {
                    await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                }
            }

            if (response == null) continue;

            var piiArray = new JsonArray();
            foreach (var entity in response.Entities)
            {
                piiArray.Add(new JsonObject
                {
                    ["type"] = entity.Type.Value,
                    ["score"] = entity.Score,
                    ["beginOffset"] = entity.BeginOffset,
                    ["endOffset"] = entity.EndOffset
                });
            }
            analysis["piiEntities"] = piiArray;
        }
    }

    private async Task ProcessSyntaxAsync(List<string> texts, Dictionary<int, JsonObject> results, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var request = new BatchDetectSyntaxRequest
        {
            TextList = texts,
            LanguageCode = _language
        };

        BatchDetectSyntaxResponse? response = null;
        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                response = await _client.BatchDetectSyntaxAsync(request, cancellationToken);
                break;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }

        if (response == null) return;

        foreach (var result in response.ResultList)
        {
            if (results.TryGetValue(result.Index.GetValueOrDefault(), out var analysis))
            {
                var tokensArray = new JsonArray();
                foreach (var token in result.SyntaxTokens)
                {
                    tokensArray.Add(new JsonObject
                    {
                        ["tokenId"] = token.TokenId,
                        ["text"] = token.Text,
                        ["partOfSpeech"] = token.PartOfSpeech?.Tag?.Value,
                        ["beginOffset"] = token.BeginOffset,
                        ["endOffset"] = token.EndOffset
                    });
                }
                analysis["syntaxTokens"] = tokensArray;
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

    private JsonObject BuildOutput(SinkRecord record, JsonObject analysis)
    {
        var output = new JsonObject();

        if (_includeOriginal && _outputFormat == ComprehendConnectorConfig.FormatMerge)
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
            _client?.Dispose();
            _client = null;
            _httpClient?.Dispose();
            _httpClient = null;
        }
        base.Dispose(disposing);
    }
}
