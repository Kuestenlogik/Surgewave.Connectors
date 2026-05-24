using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.SpaCy;

/// <summary>
/// Task that processes text using spaCy NLP server.
/// </summary>
[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance", Justification = "Interface used for extensibility")]
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "HttpClient disposed in Dispose()")]
[SuppressMessage("Usage", "CA2234:Pass System.Uri objects instead of strings", Justification = "URL strings are simpler for REST API calls")]
public sealed class SpaCySinkTask : SinkTask
{
    private HttpClient? _httpClient;
    private string _serverUrl = null!;
    private string _model = null!;
    private string _textField = null!;
    private HashSet<string> _operations = null!;
    private bool _includeText;
    private bool _includeVectors;
    private string[]? _disablePipeline;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _serverUrl = (config.TryGetValue(SpaCyConnectorConfig.ServerUrl, out var serverUrl)
            ? serverUrl : SpaCyConnectorConfig.DefaultServerUrl).TrimEnd('/');
        _model = config.TryGetValue(SpaCyConnectorConfig.Model, out var model)
            ? model : SpaCyConnectorConfig.DefaultModel;
        _textField = config.TryGetValue(SpaCyConnectorConfig.TextField, out var textField)
            ? textField : SpaCyConnectorConfig.DefaultTextField;
        _includeText = (config.TryGetValue(SpaCyConnectorConfig.IncludeText, out var includeText) ? includeText : "true") == "true";
        _includeVectors = (config.TryGetValue(SpaCyConnectorConfig.IncludeVectors, out var includeVectors) ? includeVectors : "false") == "true";

        var operationsStr = config.TryGetValue(SpaCyConnectorConfig.Operations, out var operations)
            ? operations : SpaCyConnectorConfig.DefaultOperations;
        _operations = operationsStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var disableStr = config.TryGetValue(SpaCyConnectorConfig.DisablePipeline, out var disablePipeline) ? disablePipeline : "";
        if (!string.IsNullOrWhiteSpace(disableStr))
        {
            _disablePipeline = disableStr.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        }

        _httpClient = new HttpClient();
        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        foreach (var record in records)
        {
            if (record.Value == null) continue;

            try
            {
                // Extract text from record
                string? text = null;

                try
                {
                    using var doc = JsonDocument.Parse(record.Value);
                    if (doc.RootElement.TryGetProperty(_textField, out var textProp))
                    {
                        text = textProp.GetString();
                    }
                }
                catch
                {
                    // Not JSON, treat as plain text
                    text = Encoding.UTF8.GetString(record.Value);
                }

                if (string.IsNullOrWhiteSpace(text)) continue;

                // Build request for spaCy server
                var request = new
                {
                    text,
                    model = _model,
                    disable = _disablePipeline ?? Array.Empty<string>()
                };

                var content = new StringContent(JsonSerializer.Serialize(request), Encoding.UTF8, "application/json");
                var response = await _httpClient!.PostAsync($"{_serverUrl}/process", content, cancellationToken);

                if (!response.IsSuccessStatusCode) continue;

                var resultJson = await response.Content.ReadAsStringAsync(cancellationToken);
                using var resultDoc = JsonDocument.Parse(resultJson);

                // Build output based on requested operations
                var output = BuildOutput(text, resultDoc.RootElement);

                // The output would be written to the output topic via the task context
                // In a full implementation, this would use a producer to write to the output topic
            }
            catch (Exception)
            {
                // Log and continue
            }
        }
    }

    private object BuildOutput(string text, JsonElement result)
    {
        var output = new Dictionary<string, object?>();

        if (_includeText)
        {
            output["text"] = text;
        }

        // Tokenization
        if (_operations.Contains("tokenize") && result.TryGetProperty("tokens", out var tokens))
        {
            output["tokens"] = ExtractTokens(tokens);
        }

        // Named Entity Recognition
        if (_operations.Contains("ner") && result.TryGetProperty("ents", out var ents))
        {
            output["entities"] = ExtractEntities(ents);
        }

        // Part-of-Speech tagging
        if (_operations.Contains("pos") && result.TryGetProperty("tokens", out var posTokens))
        {
            output["pos_tags"] = ExtractPosTags(posTokens);
        }

        // Lemmatization
        if (_operations.Contains("lemma") && result.TryGetProperty("tokens", out var lemmaTokens))
        {
            output["lemmas"] = ExtractLemmas(lemmaTokens);
        }

        // Dependency parsing
        if (_operations.Contains("dep") && result.TryGetProperty("tokens", out var depTokens))
        {
            output["dependencies"] = ExtractDependencies(depTokens);
        }

        // Sentence segmentation
        if (result.TryGetProperty("sents", out var sents))
        {
            output["sentences"] = ExtractSentences(sents);
        }

        // Noun chunks
        if (result.TryGetProperty("noun_chunks", out var chunks))
        {
            output["noun_chunks"] = ExtractNounChunks(chunks);
        }

        // Word vectors
        if (_includeVectors && result.TryGetProperty("vectors", out var vectors))
        {
            output["vectors"] = vectors;
        }

        return output;
    }

    private static List<string> ExtractTokens(JsonElement tokens)
    {
        var result = new List<string>();
        foreach (var token in tokens.EnumerateArray())
        {
            if (token.TryGetProperty("text", out var text))
            {
                result.Add(text.GetString() ?? "");
            }
        }
        return result;
    }

    private static List<object> ExtractEntities(JsonElement ents)
    {
        var result = new List<object>();
        foreach (var ent in ents.EnumerateArray())
        {
            result.Add(new
            {
                text = ent.TryGetProperty("text", out var t) ? t.GetString() : null,
                label = ent.TryGetProperty("label", out var l) ? l.GetString() : null,
                start = ent.TryGetProperty("start", out var s) ? s.GetInt32() : 0,
                end = ent.TryGetProperty("end", out var e) ? e.GetInt32() : 0
            });
        }
        return result;
    }

    private static List<object> ExtractPosTags(JsonElement tokens)
    {
        var result = new List<object>();
        foreach (var token in tokens.EnumerateArray())
        {
            result.Add(new
            {
                text = token.TryGetProperty("text", out var t) ? t.GetString() : null,
                pos = token.TryGetProperty("pos", out var p) ? p.GetString() : null,
                tag = token.TryGetProperty("tag", out var tag) ? tag.GetString() : null
            });
        }
        return result;
    }

    private static List<object> ExtractLemmas(JsonElement tokens)
    {
        var result = new List<object>();
        foreach (var token in tokens.EnumerateArray())
        {
            result.Add(new
            {
                text = token.TryGetProperty("text", out var t) ? t.GetString() : null,
                lemma = token.TryGetProperty("lemma", out var l) ? l.GetString() : null
            });
        }
        return result;
    }

    private static List<object> ExtractDependencies(JsonElement tokens)
    {
        var result = new List<object>();
        foreach (var token in tokens.EnumerateArray())
        {
            result.Add(new
            {
                text = token.TryGetProperty("text", out var t) ? t.GetString() : null,
                dep = token.TryGetProperty("dep", out var d) ? d.GetString() : null,
                head = token.TryGetProperty("head", out var h) ? h.GetInt32() : 0
            });
        }
        return result;
    }

    private static List<object> ExtractSentences(JsonElement sents)
    {
        var result = new List<object>();
        foreach (var sent in sents.EnumerateArray())
        {
            result.Add(new
            {
                text = sent.TryGetProperty("text", out var t) ? t.GetString() : null,
                start = sent.TryGetProperty("start", out var s) ? s.GetInt32() : 0,
                end = sent.TryGetProperty("end", out var e) ? e.GetInt32() : 0
            });
        }
        return result;
    }

    private static List<object> ExtractNounChunks(JsonElement chunks)
    {
        var result = new List<object>();
        foreach (var chunk in chunks.EnumerateArray())
        {
            result.Add(new
            {
                text = chunk.TryGetProperty("text", out var t) ? t.GetString() : null,
                root = chunk.TryGetProperty("root", out var r) ? r.GetString() : null,
                start = chunk.TryGetProperty("start", out var s) ? s.GetInt32() : 0,
                end = chunk.TryGetProperty("end", out var e) ? e.GetInt32() : 0
            });
        }
        return result;
    }

    public override Task FlushAsync(IDictionary<TopicPartition, long> currentOffsets, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public override void Stop()
    {
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _httpClient?.Dispose();
        }
        base.Dispose(disposing);
    }
}
