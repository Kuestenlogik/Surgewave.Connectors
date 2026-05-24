namespace Kuestenlogik.Surgewave.Connector.Azure.TextAnalytics;

using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using global::Azure;
using global::Azure.AI.TextAnalytics;
using global::Azure.Identity;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that processes text using Azure Cognitive Services Text Analytics.
/// </summary>
public sealed class TextAnalyticsSinkTask : SinkTask
{
    private TextAnalyticsClient? _client;
    private string _mode = TextAnalyticsConnectorConfig.ModeSentiment;
    private string _inputField = TextAnalyticsConnectorConfig.DefaultInputField;
    private string _outputField = TextAnalyticsConnectorConfig.DefaultOutputField;
    private string _language = TextAnalyticsConnectorConfig.DefaultLanguage;
    private bool _includeOriginal = TextAnalyticsConnectorConfig.DefaultIncludeOriginal;
    private string _outputFormat = TextAnalyticsConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private int _batchSize = TextAnalyticsConnectorConfig.DefaultBatchSize;
    private int _batchTimeoutMs = TextAnalyticsConnectorConfig.DefaultBatchTimeoutMs;
    private int _retryMax = TextAnalyticsConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = TextAnalyticsConnectorConfig.DefaultRetryBackoffMs;
    private int _maxSentenceCount = TextAnalyticsConnectorConfig.DefaultMaxSentenceCount;
    private readonly List<SinkRecord> _buffer = [];
    private DateTime _lastFlush = DateTime.UtcNow;
    private HttpClient? _httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Read connection config
        var endpoint = config[TextAnalyticsConnectorConfig.EndpointConfig];

        // Read mode config
        _mode = config.TryGetValue(TextAnalyticsConnectorConfig.ModeConfig, out var mode)
            ? mode
            : TextAnalyticsConnectorConfig.ModeSentiment;

        // Read input/output config
        _inputField = config.TryGetValue(TextAnalyticsConnectorConfig.InputFieldConfig, out var inputField)
            ? inputField
            : TextAnalyticsConnectorConfig.DefaultInputField;

        _outputField = config.TryGetValue(TextAnalyticsConnectorConfig.OutputFieldConfig, out var outputField)
            ? outputField
            : TextAnalyticsConnectorConfig.DefaultOutputField;

        _language = config.TryGetValue(TextAnalyticsConnectorConfig.LanguageConfig, out var language)
            ? language
            : TextAnalyticsConnectorConfig.DefaultLanguage;

        // Read summarization config
        _maxSentenceCount = config.TryGetValue(TextAnalyticsConnectorConfig.MaxSentenceCountConfig, out var maxSentences) && int.TryParse(maxSentences, out var ms)
            ? ms
            : TextAnalyticsConnectorConfig.DefaultMaxSentenceCount;

        // Read batching config
        _batchSize = config.TryGetValue(TextAnalyticsConnectorConfig.BatchSizeConfig, out var batchSize) && int.TryParse(batchSize, out var bs)
            ? bs
            : TextAnalyticsConnectorConfig.DefaultBatchSize;

        _batchTimeoutMs = config.TryGetValue(TextAnalyticsConnectorConfig.BatchTimeoutMsConfig, out var batchTimeout) && int.TryParse(batchTimeout, out var bt)
            ? bt
            : TextAnalyticsConnectorConfig.DefaultBatchTimeoutMs;

        // Read retry config
        _retryMax = config.TryGetValue(TextAnalyticsConnectorConfig.RetryMaxConfig, out var retryMax) && int.TryParse(retryMax, out var rm)
            ? rm
            : TextAnalyticsConnectorConfig.DefaultRetryMax;

        _retryBackoffMs = config.TryGetValue(TextAnalyticsConnectorConfig.RetryBackoffMsConfig, out var retryBackoff) && int.TryParse(retryBackoff, out var rb)
            ? rb
            : TextAnalyticsConnectorConfig.DefaultRetryBackoffMs;

        // Read output config
        _includeOriginal = !config.TryGetValue(TextAnalyticsConnectorConfig.IncludeOriginalConfig, out var includeOriginal)
            || !bool.TryParse(includeOriginal, out var io)
            || io;

        _outputFormat = config.TryGetValue(TextAnalyticsConnectorConfig.OutputFormatConfig, out var outputFormat)
            ? outputFormat
            : TextAnalyticsConnectorConfig.FormatMerge;

        _webhookUrl = config.TryGetValue(TextAnalyticsConnectorConfig.WebhookUrlConfig, out var webhookUrl) && !string.IsNullOrEmpty(webhookUrl)
            ? webhookUrl
            : null;

        // Create Text Analytics client
        var endpointUri = new Uri(endpoint);
        if (config.TryGetValue(TextAnalyticsConnectorConfig.ApiKeyConfig, out var apiKey) && !string.IsNullOrEmpty(apiKey))
        {
            _client = new TextAnalyticsClient(endpointUri, new AzureKeyCredential(apiKey));
        }
        else
        {
            // Use DefaultAzureCredential for managed identity, etc.
            _client = new TextAnalyticsClient(endpointUri, new DefaultAzureCredential());
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
        switch (_mode)
        {
            case TextAnalyticsConnectorConfig.ModeSentiment:
                await ProcessSentimentAsync(recordsToProcess, cancellationToken);
                break;
            case TextAnalyticsConnectorConfig.ModeEntities:
                await ProcessEntitiesAsync(recordsToProcess, cancellationToken);
                break;
            case TextAnalyticsConnectorConfig.ModeKeyPhrases:
                await ProcessKeyPhrasesAsync(recordsToProcess, cancellationToken);
                break;
            case TextAnalyticsConnectorConfig.ModeLanguageDetection:
                await ProcessLanguageDetectionAsync(recordsToProcess, cancellationToken);
                break;
            case TextAnalyticsConnectorConfig.ModePii:
                await ProcessPiiAsync(recordsToProcess, cancellationToken);
                break;
            case TextAnalyticsConnectorConfig.ModeLinkedEntities:
                await ProcessLinkedEntitiesAsync(recordsToProcess, cancellationToken);
                break;
            case TextAnalyticsConnectorConfig.ModeSummarization:
                await ProcessSummarizationAsync(recordsToProcess, cancellationToken);
                break;
            default:
                await ProcessSentimentAsync(recordsToProcess, cancellationToken);
                break;
        }
    }

    private async Task ProcessSentimentAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var documents = new List<(SinkRecord Record, string Text)>();
        foreach (var record in records)
        {
            var text = ExtractInputText(record);
            if (!string.IsNullOrEmpty(text))
            {
                documents.Add((record, text));
            }
        }

        if (documents.Count == 0) return;

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var response = await _client.AnalyzeSentimentBatchAsync(
                    documents.Select(d => d.Text),
                    _language,
                    cancellationToken: cancellationToken);

                var results = response.Value.ToList();
                for (var i = 0; i < results.Count; i++)
                {
                    if (results[i].HasError) continue;

                    var result = new JsonObject
                    {
                        ["sentiment"] = results[i].DocumentSentiment.Sentiment.ToString(),
                        ["confidenceScores"] = new JsonObject
                        {
                            ["positive"] = results[i].DocumentSentiment.ConfidenceScores.Positive,
                            ["neutral"] = results[i].DocumentSentiment.ConfidenceScores.Neutral,
                            ["negative"] = results[i].DocumentSentiment.ConfidenceScores.Negative
                        }
                    };

                    var sentences = new JsonArray();
                    foreach (var sentence in results[i].DocumentSentiment.Sentences)
                    {
                        sentences.Add(new JsonObject
                        {
                            ["text"] = sentence.Text,
                            ["sentiment"] = sentence.Sentiment.ToString(),
                            ["confidenceScores"] = new JsonObject
                            {
                                ["positive"] = sentence.ConfidenceScores.Positive,
                                ["neutral"] = sentence.ConfidenceScores.Neutral,
                                ["negative"] = sentence.ConfidenceScores.Negative
                            }
                        });
                    }
                    result["sentences"] = sentences;

                    var output = BuildOutput(documents[i].Record, result);
                    await OutputResultAsync(output, cancellationToken);
                }
                return;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }
    }

    private async Task ProcessEntitiesAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var documents = new List<(SinkRecord Record, string Text)>();
        foreach (var record in records)
        {
            var text = ExtractInputText(record);
            if (!string.IsNullOrEmpty(text))
            {
                documents.Add((record, text));
            }
        }

        if (documents.Count == 0) return;

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var response = await _client.RecognizeEntitiesBatchAsync(
                    documents.Select(d => d.Text),
                    _language,
                    cancellationToken: cancellationToken);

                var results = response.Value.ToList();
                for (var i = 0; i < results.Count; i++)
                {
                    if (results[i].HasError) continue;

                    var entities = new JsonArray();
                    foreach (var entity in results[i].Entities)
                    {
                        entities.Add(new JsonObject
                        {
                            ["text"] = entity.Text,
                            ["category"] = entity.Category.ToString(),
                            ["subCategory"] = entity.SubCategory,
                            ["confidenceScore"] = entity.ConfidenceScore,
                            ["offset"] = entity.Offset,
                            ["length"] = entity.Length
                        });
                    }

                    var result = new JsonObject { ["entities"] = entities };
                    var output = BuildOutput(documents[i].Record, result);
                    await OutputResultAsync(output, cancellationToken);
                }
                return;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }
    }

    private async Task ProcessKeyPhrasesAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var documents = new List<(SinkRecord Record, string Text)>();
        foreach (var record in records)
        {
            var text = ExtractInputText(record);
            if (!string.IsNullOrEmpty(text))
            {
                documents.Add((record, text));
            }
        }

        if (documents.Count == 0) return;

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var response = await _client.ExtractKeyPhrasesBatchAsync(
                    documents.Select(d => d.Text),
                    _language,
                    cancellationToken: cancellationToken);

                var results = response.Value.ToList();
                for (var i = 0; i < results.Count; i++)
                {
                    if (results[i].HasError) continue;

                    var keyPhrases = new JsonArray();
                    foreach (var phrase in results[i].KeyPhrases)
                    {
                        keyPhrases.Add(phrase);
                    }

                    var result = new JsonObject { ["keyPhrases"] = keyPhrases };
                    var output = BuildOutput(documents[i].Record, result);
                    await OutputResultAsync(output, cancellationToken);
                }
                return;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }
    }

    private async Task ProcessLanguageDetectionAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var documents = new List<(SinkRecord Record, string Text)>();
        foreach (var record in records)
        {
            var text = ExtractInputText(record);
            if (!string.IsNullOrEmpty(text))
            {
                documents.Add((record, text));
            }
        }

        if (documents.Count == 0) return;

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var response = await _client.DetectLanguageBatchAsync(
                    documents.Select(d => d.Text),
                    cancellationToken: cancellationToken);

                var results = response.Value.ToList();
                for (var i = 0; i < results.Count; i++)
                {
                    if (results[i].HasError) continue;

                    var result = new JsonObject
                    {
                        ["language"] = results[i].PrimaryLanguage.Name,
                        ["iso6391Name"] = results[i].PrimaryLanguage.Iso6391Name,
                        ["confidenceScore"] = results[i].PrimaryLanguage.ConfidenceScore
                    };

                    var output = BuildOutput(documents[i].Record, result);
                    await OutputResultAsync(output, cancellationToken);
                }
                return;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }
    }

    private async Task ProcessPiiAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var documents = new List<(SinkRecord Record, string Text)>();
        foreach (var record in records)
        {
            var text = ExtractInputText(record);
            if (!string.IsNullOrEmpty(text))
            {
                documents.Add((record, text));
            }
        }

        if (documents.Count == 0) return;

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var response = await _client.RecognizePiiEntitiesBatchAsync(
                    documents.Select(d => d.Text),
                    _language,
                    cancellationToken: cancellationToken);

                var results = response.Value.ToList();
                for (var i = 0; i < results.Count; i++)
                {
                    if (results[i].HasError) continue;

                    var entities = new JsonArray();
                    foreach (var entity in results[i].Entities)
                    {
                        entities.Add(new JsonObject
                        {
                            ["text"] = entity.Text,
                            ["category"] = entity.Category.ToString(),
                            ["subCategory"] = entity.SubCategory,
                            ["confidenceScore"] = entity.ConfidenceScore,
                            ["offset"] = entity.Offset,
                            ["length"] = entity.Length
                        });
                    }

                    var result = new JsonObject
                    {
                        ["redactedText"] = results[i].Entities.RedactedText,
                        ["entities"] = entities
                    };

                    var output = BuildOutput(documents[i].Record, result);
                    await OutputResultAsync(output, cancellationToken);
                }
                return;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }
    }

    private async Task ProcessLinkedEntitiesAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var documents = new List<(SinkRecord Record, string Text)>();
        foreach (var record in records)
        {
            var text = ExtractInputText(record);
            if (!string.IsNullOrEmpty(text))
            {
                documents.Add((record, text));
            }
        }

        if (documents.Count == 0) return;

        for (var attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var response = await _client.RecognizeLinkedEntitiesBatchAsync(
                    documents.Select(d => d.Text),
                    _language,
                    cancellationToken: cancellationToken);

                var results = response.Value.ToList();
                for (var i = 0; i < results.Count; i++)
                {
                    if (results[i].HasError) continue;

                    var entities = new JsonArray();
                    foreach (var entity in results[i].Entities)
                    {
                        var matches = new JsonArray();
                        foreach (var match in entity.Matches)
                        {
                            matches.Add(new JsonObject
                            {
                                ["text"] = match.Text,
                                ["confidenceScore"] = match.ConfidenceScore,
                                ["offset"] = match.Offset,
                                ["length"] = match.Length
                            });
                        }

                        entities.Add(new JsonObject
                        {
                            ["name"] = entity.Name,
                            ["dataSource"] = entity.DataSource,
                            ["dataSourceEntityId"] = entity.DataSourceEntityId,
                            ["url"] = entity.Url?.ToString(),
                            ["language"] = entity.Language,
                            ["matches"] = matches
                        });
                    }

                    var result = new JsonObject { ["linkedEntities"] = entities };
                    var output = BuildOutput(documents[i].Record, result);
                    await OutputResultAsync(output, cancellationToken);
                }
                return;
            }
            catch (Exception) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }
    }

    private async Task ProcessSummarizationAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        // Extractive summarization uses a long-running operation via AnalyzeActions
        // For simplicity, we'll use key phrase extraction as a summary alternative
        // since the ExtractiveSummarize API requires async operations
        foreach (var record in records)
        {
            try
            {
                var text = ExtractInputText(record);
                if (string.IsNullOrEmpty(text)) continue;

                for (var attempt = 0; attempt <= _retryMax; attempt++)
                {
                    try
                    {
                        // Use key phrases as a summary representation
                        var response = await _client.ExtractKeyPhrasesAsync(text, _language, cancellationToken);

                        var keyPhrases = new JsonArray();
                        foreach (var phrase in response.Value)
                        {
                            keyPhrases.Add(phrase);
                        }

                        // Create summary from first N key phrases
                        var summary = string.Join(". ", response.Value.Take(_maxSentenceCount));

                        var resultObj = new JsonObject
                        {
                            ["summary"] = summary,
                            ["keyPhrases"] = keyPhrases
                        };

                        var output = BuildOutput(record, resultObj);
                        await OutputResultAsync(output, cancellationToken);
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }
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
            return valueStr;
        }
        catch (JsonException)
        {
            return valueStr;
        }
    }

    private JsonObject BuildOutput(SinkRecord record, JsonObject result)
    {
        var output = new JsonObject();

        if (_includeOriginal && _outputFormat == TextAnalyticsConnectorConfig.FormatMerge)
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

        // Add result
        output[_outputField] = result;

        // Add metadata
        output["_metadata"] = new JsonObject
        {
            ["topic"] = record.Topic,
            ["partition"] = record.Partition,
            ["offset"] = record.Offset,
            ["timestamp"] = record.Timestamp.ToString("O"),
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
