namespace Kuestenlogik.Surgewave.Connector.DeepL;

using System.Text.Json;
using System.Text.Json.Nodes;
using global::DeepL;
using global::DeepL.Model;
using Kuestenlogik.Surgewave.Connect;

/// <summary>
/// A sink task that processes text using the DeepL API.
/// Supports translation, language detection, and usage monitoring.
/// </summary>
public sealed class DeepLSinkTask : SinkTask
{
    private static readonly JsonSerializerOptions SerializerOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    private Translator? _translator;
    private string _mode = DeepLConnectorConfig.ModeTranslate;
    private string _inputField = DeepLConnectorConfig.DefaultInputField;
    private string _outputField = DeepLConnectorConfig.DefaultOutputField;
    private string _sourceLanguage = DeepLConnectorConfig.DefaultSourceLanguage;
    private string _targetLanguage = DeepLConnectorConfig.DefaultTargetLanguage;
    private string _formality = DeepLConnectorConfig.FormalityDefault;
    private string _context = DeepLConnectorConfig.DefaultContext;
    private string? _glossaryId;
    private string? _tagHandling;
    private bool _preserveFormatting = DeepLConnectorConfig.DefaultPreserveFormatting;
    private string _splitSentences = DeepLConnectorConfig.SplitSentencesAll;
    private int _batchSize = DeepLConnectorConfig.DefaultBatchSize;
    private int _retryMax = DeepLConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = DeepLConnectorConfig.DefaultRetryBackoffMs;
    private bool _includeOriginal = DeepLConnectorConfig.DefaultIncludeOriginal;
    private bool _includeDetectedLanguage = DeepLConnectorConfig.DefaultIncludeDetectedLanguage;
    private string _outputFormat = DeepLConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private HttpClient? _httpClient;
    private bool _disposed;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var apiKey = config[DeepLConnectorConfig.ApiKeyConfig];

        // Configure translator options
        var options = new TranslatorOptions();
        if (config.TryGetValue(DeepLConnectorConfig.ServerUrlConfig, out var serverUrl) && !string.IsNullOrWhiteSpace(serverUrl))
        {
            options.ServerUrl = serverUrl;
        }

        _translator = new Translator(apiKey, options);

        if (config.TryGetValue(DeepLConnectorConfig.ModeConfig, out var mode))
            _mode = mode;

        if (config.TryGetValue(DeepLConnectorConfig.InputFieldConfig, out var inputField))
            _inputField = inputField;

        if (config.TryGetValue(DeepLConnectorConfig.OutputFieldConfig, out var outputField))
            _outputField = outputField;

        if (config.TryGetValue(DeepLConnectorConfig.SourceLanguageConfig, out var srcLang))
            _sourceLanguage = srcLang;

        if (config.TryGetValue(DeepLConnectorConfig.TargetLanguageConfig, out var tgtLang))
            _targetLanguage = tgtLang;

        if (config.TryGetValue(DeepLConnectorConfig.FormalityConfig, out var formality))
            _formality = formality;

        if (config.TryGetValue(DeepLConnectorConfig.ContextConfig, out var context))
            _context = context;

        if (config.TryGetValue(DeepLConnectorConfig.GlossaryIdConfig, out var glossaryId) && !string.IsNullOrWhiteSpace(glossaryId))
            _glossaryId = glossaryId;

        if (config.TryGetValue(DeepLConnectorConfig.TagHandlingConfig, out var tagHandling) && !string.IsNullOrWhiteSpace(tagHandling))
            _tagHandling = tagHandling;

        if (config.TryGetValue(DeepLConnectorConfig.PreserveFormattingConfig, out var preserveFormatting))
            _preserveFormatting = bool.Parse(preserveFormatting);

        if (config.TryGetValue(DeepLConnectorConfig.SplitSentencesConfig, out var splitSentences))
            _splitSentences = splitSentences;

        if (config.TryGetValue(DeepLConnectorConfig.BatchSizeConfig, out var batchSizeStr))
            _batchSize = int.Parse(batchSizeStr);

        if (config.TryGetValue(DeepLConnectorConfig.RetryMaxConfig, out var retryMaxStr))
            _retryMax = int.Parse(retryMaxStr);

        if (config.TryGetValue(DeepLConnectorConfig.RetryBackoffMsConfig, out var retryBackoffStr))
            _retryBackoffMs = int.Parse(retryBackoffStr);

        if (config.TryGetValue(DeepLConnectorConfig.IncludeOriginalConfig, out var includeOriginal))
            _includeOriginal = bool.Parse(includeOriginal);

        if (config.TryGetValue(DeepLConnectorConfig.IncludeDetectedLanguageConfig, out var includeDetectedLang))
            _includeDetectedLanguage = bool.Parse(includeDetectedLang);

        if (config.TryGetValue(DeepLConnectorConfig.OutputFormatConfig, out var outputFormat))
            _outputFormat = outputFormat;

        if (config.TryGetValue(DeepLConnectorConfig.WebhookUrlConfig, out var webhookUrl) && !string.IsNullOrWhiteSpace(webhookUrl))
        {
            _webhookUrl = webhookUrl;
            _httpClient = new HttpClient();
        }
    }

    public override void Stop()
    {
        _httpClient?.Dispose();
        _httpClient = null;
        _translator?.Dispose();
        _translator = null;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (records.Count == 0) return;

        // Process in batches
        for (var i = 0; i < records.Count; i += _batchSize)
        {
            var batch = records.Skip(i).Take(_batchSize).ToList();
            await ProcessBatchAsync(batch, cancellationToken);
        }
    }

    private async Task ProcessBatchAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        switch (_mode)
        {
            case DeepLConnectorConfig.ModeTranslate:
                await ProcessTranslateAsync(records, cancellationToken);
                break;
            case DeepLConnectorConfig.ModeDetectLanguage:
                await ProcessDetectLanguageAsync(records, cancellationToken);
                break;
            case DeepLConnectorConfig.ModeUsage:
                await ProcessUsageAsync(cancellationToken);
                break;
        }
    }

    private async Task ProcessTranslateAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_translator == null) return;

        // Extract texts from records
        var texts = new List<string>();
        var recordMap = new List<(SinkRecord Record, JsonObject? Original)>();

        foreach (var record in records)
        {
            var (text, original) = ExtractText(record);
            if (text != null)
            {
                texts.Add(text);
                recordMap.Add((record, original));
            }
        }

        if (texts.Count == 0) return;

        // Build translation options
        var options = new TextTranslateOptions
        {
            PreserveFormatting = _preserveFormatting
        };

        // Set formality
        if (!string.IsNullOrWhiteSpace(_formality) && _formality != DeepLConnectorConfig.FormalityDefault)
        {
            options.Formality = _formality switch
            {
                DeepLConnectorConfig.FormalityMore => Formality.More,
                DeepLConnectorConfig.FormalityLess => Formality.Less,
                DeepLConnectorConfig.FormalityPreferMore => Formality.PreferMore,
                DeepLConnectorConfig.FormalityPreferLess => Formality.PreferLess,
                _ => Formality.Default
            };
        }

        // Set context if provided
        if (!string.IsNullOrWhiteSpace(_context))
        {
            options.Context = _context;
        }

        // Set glossary if provided
        if (!string.IsNullOrWhiteSpace(_glossaryId))
        {
            options.GlossaryId = _glossaryId;
        }

        // Set tag handling if provided
        if (!string.IsNullOrWhiteSpace(_tagHandling))
        {
            options.TagHandling = _tagHandling;
        }

        // Set sentence splitting
        options.SentenceSplittingMode = _splitSentences switch
        {
            DeepLConnectorConfig.SplitSentencesNone => SentenceSplittingMode.Off,
            DeepLConnectorConfig.SplitSentencesPunctuation => SentenceSplittingMode.NoNewlines,
            _ => SentenceSplittingMode.All
        };

        // Translate with retry
        TextResult[]? translations = null;
        for (var attempt = 0; attempt <= _retryMax && translations == null; attempt++)
        {
            try
            {
                translations = await _translator.TranslateTextAsync(
                    texts,
                    string.IsNullOrWhiteSpace(_sourceLanguage) ? null : _sourceLanguage,
                    _targetLanguage,
                    options,
                    cancellationToken);
            }
            catch (DeepLException) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
            }
        }

        if (translations == null) return;

        // Build output
        for (var i = 0; i < translations.Length && i < recordMap.Count; i++)
        {
            var (record, original) = recordMap[i];
            var translation = translations[i];

            var result = new JsonObject
            {
                ["translatedText"] = translation.Text,
                ["targetLanguage"] = _targetLanguage
            };

            if (_includeDetectedLanguage)
            {
                result["detectedSourceLanguage"] = translation.DetectedSourceLanguageCode;
            }

            await OutputResultAsync(record, original, result);
        }
    }

    private async Task ProcessDetectLanguageAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_translator == null) return;

        // DeepL doesn't have a dedicated language detection API
        // We use a dummy translation to detect the language
        foreach (var record in records)
        {
            var (text, original) = ExtractText(record);
            if (text == null) continue;

            // Translate to a target language to detect source
            TextResult? translation = null;
            for (var attempt = 0; attempt <= _retryMax && translation == null; attempt++)
            {
                try
                {
                    translation = await _translator.TranslateTextAsync(
                        text.Substring(0, Math.Min(text.Length, 100)), // Use short sample
                        null, // Auto-detect
                        "EN-US",
                        cancellationToken: cancellationToken);
                }
                catch (DeepLException) when (attempt < _retryMax)
                {
                    await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
                }
            }

            if (translation == null) continue;

            var result = new JsonObject
            {
                ["detectedLanguage"] = translation.DetectedSourceLanguageCode,
                ["sampleText"] = text.Substring(0, Math.Min(text.Length, 100))
            };

            await OutputResultAsync(record, original, result);
        }
    }

    private async Task ProcessUsageAsync(CancellationToken cancellationToken)
    {
        if (_translator == null) return;

        Usage? usage = null;
        for (var attempt = 0; attempt <= _retryMax && usage == null; attempt++)
        {
            try
            {
                usage = await _translator.GetUsageAsync(cancellationToken);
            }
            catch (DeepLException) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (int)Math.Pow(2, attempt), cancellationToken);
            }
        }

        if (usage == null) return;

        var result = new JsonObject
        {
            ["characterCount"] = usage.Character?.Count,
            ["characterLimit"] = usage.Character?.Limit,
            ["documentCount"] = usage.Document?.Count,
            ["documentLimit"] = usage.Document?.Limit,
            ["teamDocumentCount"] = usage.TeamDocument?.Count,
            ["teamDocumentLimit"] = usage.TeamDocument?.Limit
        };

        await OutputResultAsync(null, null, result);
    }

    private (string? Text, JsonObject? Original) ExtractText(SinkRecord record)
    {
        if (record.Value == null) return (null, null);

        try
        {
            var json = JsonSerializer.Deserialize<JsonObject>(record.Value);
            if (json == null) return (null, null);

            var text = json[_inputField]?.GetValue<string>();
            return (text, json);
        }
        catch
        {
            // Try as plain text
            var text = System.Text.Encoding.UTF8.GetString(record.Value);
            return (text, null);
        }
    }

    private async Task OutputResultAsync(SinkRecord? record, JsonObject? original, JsonObject result)
    {
        JsonObject output;

        if (_outputFormat == DeepLConnectorConfig.FormatMerge && original != null && _includeOriginal)
        {
            output = JsonSerializer.Deserialize<JsonObject>(original.ToJsonString()) ?? new JsonObject();
            output[_outputField] = result;
        }
        else
        {
            output = new JsonObject
            {
                [_outputField] = result
            };

            if (_includeOriginal && record != null)
            {
                output["topic"] = record.Topic;
                output["partition"] = record.Partition;
                output["offset"] = record.Offset;
            }
        }

        var json = output.ToJsonString(SerializerOptions);

        if (_webhookUrl != null && _httpClient != null)
        {
            using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            await _httpClient.PostAsync(new Uri(_webhookUrl), content);
        }
        else
        {
            Console.WriteLine(json);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                _httpClient?.Dispose();
                _translator?.Dispose();
            }
            _disposed = true;
        }
        base.Dispose(disposing);
    }
}
