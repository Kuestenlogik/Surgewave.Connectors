namespace Kuestenlogik.Surgewave.Connector.Azure.OpenAI;

using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using global::Azure;
using global::Azure.AI.OpenAI;
using global::Azure.Identity;
using Kuestenlogik.Surgewave.Connect;
using global::OpenAI.Chat;
using global::OpenAI.Embeddings;
using global::OpenAI.Images;
using global::OpenAI.Audio;

/// <summary>
/// A sink task that processes data using Azure OpenAI Service.
/// </summary>
public sealed class AzureOpenAISinkTask : SinkTask
{
    private AzureOpenAIClient? _client;
    private string _deploymentId = "";
    private string _mode = AzureOpenAIConnectorConfig.ModeChat;
    private string _systemPrompt = "";
    private int _maxTokens = AzureOpenAIConnectorConfig.DefaultMaxTokens;
    private float _temperature = (float)AzureOpenAIConnectorConfig.DefaultTemperature;
    private float _topP = (float)AzureOpenAIConnectorConfig.DefaultTopP;
    private float _frequencyPenalty = (float)AzureOpenAIConnectorConfig.DefaultFrequencyPenalty;
    private float _presencePenalty = (float)AzureOpenAIConnectorConfig.DefaultPresencePenalty;
    private string _inputField = AzureOpenAIConnectorConfig.DefaultInputField;
    private string _outputField = AzureOpenAIConnectorConfig.DefaultOutputField;
    private string _embeddingsField = AzureOpenAIConnectorConfig.DefaultEmbeddingsField;
    private bool _includeOriginal = AzureOpenAIConnectorConfig.DefaultIncludeOriginal;
    private string _outputFormat = AzureOpenAIConnectorConfig.FormatMerge;
    private string? _webhookUrl;
    private int _batchSize = AzureOpenAIConnectorConfig.DefaultBatchSize;
    private int _batchTimeoutMs = AzureOpenAIConnectorConfig.DefaultBatchTimeoutMs;
    private int _retryMax = AzureOpenAIConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = AzureOpenAIConnectorConfig.DefaultRetryBackoffMs;
    private readonly List<SinkRecord> _buffer = [];
    private DateTime _lastFlush = DateTime.UtcNow;
    private HttpClient? _httpClient;

    // DALL-E settings
    private string _imageSize = AzureOpenAIConnectorConfig.DefaultImageSize;
    private string _imageQuality = AzureOpenAIConnectorConfig.DefaultImageQuality;
    private string _imageStyle = AzureOpenAIConnectorConfig.DefaultImageStyle;
    private int _imageCount = AzureOpenAIConnectorConfig.DefaultImageCount;

    // Whisper settings
    private string? _audioLanguage;
    private string _whisperMode = AzureOpenAIConnectorConfig.DefaultWhisperMode;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        // Read connection config
        var endpoint = config[AzureOpenAIConnectorConfig.EndpointConfig];
        _deploymentId = config[AzureOpenAIConnectorConfig.DeploymentIdConfig];

        // Read mode config
        _mode = config.TryGetValue(AzureOpenAIConnectorConfig.ModeConfig, out var mode)
            ? mode
            : AzureOpenAIConnectorConfig.ModeChat;

        // Read completion config
        _systemPrompt = config.TryGetValue(AzureOpenAIConnectorConfig.SystemPromptConfig, out var systemPrompt)
            ? systemPrompt
            : "";

        _maxTokens = config.TryGetValue(AzureOpenAIConnectorConfig.MaxTokensConfig, out var maxTokens) && int.TryParse(maxTokens, out var mt)
            ? mt
            : AzureOpenAIConnectorConfig.DefaultMaxTokens;

        _temperature = config.TryGetValue(AzureOpenAIConnectorConfig.TemperatureConfig, out var temp) && float.TryParse(temp, out var t)
            ? t
            : (float)AzureOpenAIConnectorConfig.DefaultTemperature;

        _topP = config.TryGetValue(AzureOpenAIConnectorConfig.TopPConfig, out var topP) && float.TryParse(topP, out var tp)
            ? tp
            : (float)AzureOpenAIConnectorConfig.DefaultTopP;

        _frequencyPenalty = config.TryGetValue(AzureOpenAIConnectorConfig.FrequencyPenaltyConfig, out var freqPenalty) && float.TryParse(freqPenalty, out var fp)
            ? fp
            : (float)AzureOpenAIConnectorConfig.DefaultFrequencyPenalty;

        _presencePenalty = config.TryGetValue(AzureOpenAIConnectorConfig.PresencePenaltyConfig, out var presPenalty) && float.TryParse(presPenalty, out var pp)
            ? pp
            : (float)AzureOpenAIConnectorConfig.DefaultPresencePenalty;

        // Read input/output config
        _inputField = config.TryGetValue(AzureOpenAIConnectorConfig.InputFieldConfig, out var inputField)
            ? inputField
            : AzureOpenAIConnectorConfig.DefaultInputField;

        _outputField = config.TryGetValue(AzureOpenAIConnectorConfig.OutputFieldConfig, out var outputField)
            ? outputField
            : AzureOpenAIConnectorConfig.DefaultOutputField;

        _embeddingsField = config.TryGetValue(AzureOpenAIConnectorConfig.EmbeddingsFieldConfig, out var embField)
            ? embField
            : AzureOpenAIConnectorConfig.DefaultEmbeddingsField;

        // Read DALL-E config
        _imageSize = config.TryGetValue(AzureOpenAIConnectorConfig.ImageSizeConfig, out var imgSize)
            ? imgSize
            : AzureOpenAIConnectorConfig.DefaultImageSize;

        _imageQuality = config.TryGetValue(AzureOpenAIConnectorConfig.ImageQualityConfig, out var imgQuality)
            ? imgQuality
            : AzureOpenAIConnectorConfig.DefaultImageQuality;

        _imageStyle = config.TryGetValue(AzureOpenAIConnectorConfig.ImageStyleConfig, out var imgStyle)
            ? imgStyle
            : AzureOpenAIConnectorConfig.DefaultImageStyle;

        _imageCount = config.TryGetValue(AzureOpenAIConnectorConfig.ImageCountConfig, out var imgCount) && int.TryParse(imgCount, out var ic)
            ? ic
            : AzureOpenAIConnectorConfig.DefaultImageCount;

        // Read Whisper config
        _audioLanguage = config.TryGetValue(AzureOpenAIConnectorConfig.AudioLanguageConfig, out var audioLang) && !string.IsNullOrEmpty(audioLang)
            ? audioLang
            : null;

        _whisperMode = config.TryGetValue(AzureOpenAIConnectorConfig.WhisperModeConfig, out var whisperMode)
            ? whisperMode
            : AzureOpenAIConnectorConfig.DefaultWhisperMode;

        // Read batching config
        _batchSize = config.TryGetValue(AzureOpenAIConnectorConfig.BatchSizeConfig, out var batchSize) && int.TryParse(batchSize, out var bs)
            ? bs
            : AzureOpenAIConnectorConfig.DefaultBatchSize;

        _batchTimeoutMs = config.TryGetValue(AzureOpenAIConnectorConfig.BatchTimeoutMsConfig, out var batchTimeout) && int.TryParse(batchTimeout, out var bt)
            ? bt
            : AzureOpenAIConnectorConfig.DefaultBatchTimeoutMs;

        // Read retry config
        _retryMax = config.TryGetValue(AzureOpenAIConnectorConfig.RetryMaxConfig, out var retryMax) && int.TryParse(retryMax, out var rm)
            ? rm
            : AzureOpenAIConnectorConfig.DefaultRetryMax;

        _retryBackoffMs = config.TryGetValue(AzureOpenAIConnectorConfig.RetryBackoffMsConfig, out var retryBackoff) && int.TryParse(retryBackoff, out var rb)
            ? rb
            : AzureOpenAIConnectorConfig.DefaultRetryBackoffMs;

        // Read output config
        _includeOriginal = !config.TryGetValue(AzureOpenAIConnectorConfig.IncludeOriginalConfig, out var includeOriginal)
            || !bool.TryParse(includeOriginal, out var io)
            || io;

        _outputFormat = config.TryGetValue(AzureOpenAIConnectorConfig.OutputFormatConfig, out var outputFormat)
            ? outputFormat
            : AzureOpenAIConnectorConfig.FormatMerge;

        _webhookUrl = config.TryGetValue(AzureOpenAIConnectorConfig.WebhookUrlConfig, out var webhookUrl) && !string.IsNullOrEmpty(webhookUrl)
            ? webhookUrl
            : null;

        // Create Azure OpenAI client
        var endpointUri = new Uri(endpoint);
        if (config.TryGetValue(AzureOpenAIConnectorConfig.ApiKeyConfig, out var apiKey) && !string.IsNullOrEmpty(apiKey))
        {
            _client = new AzureOpenAIClient(endpointUri, new AzureKeyCredential(apiKey));
        }
        else
        {
            // Use DefaultAzureCredential for managed identity, etc.
            _client = new AzureOpenAIClient(endpointUri, new DefaultAzureCredential());
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
            case AzureOpenAIConnectorConfig.ModeEmbeddings:
                await ProcessEmbeddingsAsync(recordsToProcess, cancellationToken);
                break;
            case AzureOpenAIConnectorConfig.ModeDallE:
                await ProcessDallEAsync(recordsToProcess, cancellationToken);
                break;
            case AzureOpenAIConnectorConfig.ModeWhisper:
                await ProcessWhisperAsync(recordsToProcess, cancellationToken);
                break;
            default:
                await ProcessChatAsync(recordsToProcess, cancellationToken);
                break;
        }
    }

    private async Task ProcessChatAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var chatClient = _client.GetChatClient(_deploymentId);

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
                        var messages = new List<ChatMessage>();

                        if (!string.IsNullOrEmpty(_systemPrompt))
                        {
                            messages.Add(new SystemChatMessage(_systemPrompt));
                        }
                        messages.Add(new UserChatMessage(inputText));

                        var options = new ChatCompletionOptions
                        {
                            MaxOutputTokenCount = _maxTokens,
                            Temperature = _temperature,
                            TopP = _topP,
                            FrequencyPenalty = _frequencyPenalty,
                            PresencePenalty = _presencePenalty
                        };

                        var response = await chatClient.CompleteChatAsync(messages, options, cancellationToken);
                        responseText = response.Value.Content[0].Text;
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (responseText == null) continue;

                var output = BuildOutput(record, responseText, null, null, null);
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

        var embeddingClient = _client.GetEmbeddingClient(_deploymentId);

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
                        var response = await embeddingClient.GenerateEmbeddingAsync(inputText, cancellationToken: cancellationToken);
                        embedding = response.Value.ToFloats().ToArray();
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (embedding == null) continue;

                var output = BuildOutput(record, null, embedding, null, null);
                await OutputResultAsync(output, cancellationToken);
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
            }
        }
    }

    private async Task ProcessDallEAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var imageClient = _client.GetImageClient(_deploymentId);

        foreach (var record in records)
        {
            try
            {
                var prompt = ExtractInputText(record);
                if (string.IsNullOrEmpty(prompt)) continue;

                List<string>? imageUrls = null;
                for (var attempt = 0; attempt <= _retryMax; attempt++)
                {
                    try
                    {
                        var size = _imageSize switch
                        {
                            "1024x1792" => GeneratedImageSize.W1024xH1792,
                            "1792x1024" => GeneratedImageSize.W1792xH1024,
                            _ => GeneratedImageSize.W1024xH1024
                        };

                        var quality = _imageQuality switch
                        {
                            "hd" => GeneratedImageQuality.High,
                            _ => GeneratedImageQuality.Standard
                        };

                        var style = _imageStyle switch
                        {
                            "natural" => GeneratedImageStyle.Natural,
                            _ => GeneratedImageStyle.Vivid
                        };

                        var options = new ImageGenerationOptions
                        {
                            Size = size,
                            Quality = quality,
                            Style = style,
                            ResponseFormat = GeneratedImageFormat.Uri
                        };

                        var response = await imageClient.GenerateImagesAsync(prompt, _imageCount, options, cancellationToken);
                        imageUrls = response.Value.Select(img => img.ImageUri?.ToString() ?? "").Where(u => !string.IsNullOrEmpty(u)).ToList();
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (imageUrls == null || imageUrls.Count == 0) continue;

                var output = BuildOutput(record, null, null, imageUrls, null);
                await OutputResultAsync(output, cancellationToken);
            }
            catch (Exception ex)
            {
                Context.RaiseError?.Invoke(ex);
            }
        }
    }

    private async Task ProcessWhisperAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null) return;

        var audioClient = _client.GetAudioClient(_deploymentId);

        foreach (var record in records)
        {
            try
            {
                // Expect audio data in base64 or raw bytes
                byte[]? audioData = null;
                string? fileName = "audio.mp3";

                if (record.Value is byte[] bytes)
                {
                    audioData = bytes;
                }
                else if (record.Value != null)
                {
                    var valueStr = record.Value.ToString();
                    if (!string.IsNullOrEmpty(valueStr))
                    {
                        try
                        {
                            var doc = JsonDocument.Parse(valueStr);
                            if (doc.RootElement.TryGetProperty(_inputField, out var field))
                            {
                                var base64 = field.GetString();
                                if (!string.IsNullOrEmpty(base64))
                                {
                                    audioData = Convert.FromBase64String(base64);
                                }
                            }
                            if (doc.RootElement.TryGetProperty("filename", out var fnField))
                            {
                                fileName = fnField.GetString() ?? fileName;
                            }
                        }
                        catch (JsonException)
                        {
                            // Try as base64 directly
                            audioData = Convert.FromBase64String(valueStr);
                        }
                    }
                }

                if (audioData == null) continue;

                string? transcription = null;
                for (var attempt = 0; attempt <= _retryMax; attempt++)
                {
                    try
                    {
                        using var stream = new MemoryStream(audioData);

                        if (_whisperMode == AzureOpenAIConnectorConfig.WhisperModeTranslate)
                        {
                            var options = new AudioTranslationOptions();
                            var response = await audioClient.TranslateAudioAsync(stream, fileName, options, cancellationToken);
                            transcription = response.Value.Text;
                        }
                        else
                        {
                            var options = new AudioTranscriptionOptions
                            {
                                Language = _audioLanguage
                            };
                            var response = await audioClient.TranscribeAudioAsync(stream, fileName, options, cancellationToken);
                            transcription = response.Value.Text;
                        }
                        break;
                    }
                    catch (Exception) when (attempt < _retryMax)
                    {
                        await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                    }
                }

                if (transcription == null) continue;

                var output = BuildOutput(record, null, null, null, transcription);
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

    private JsonObject BuildOutput(SinkRecord record, string? responseText, float[]? embedding, List<string>? imageUrls, string? transcription)
    {
        var output = new JsonObject();

        if (_includeOriginal && _outputFormat == AzureOpenAIConnectorConfig.FormatMerge)
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

        if (imageUrls != null && imageUrls.Count > 0)
        {
            var urlArray = new JsonArray();
            foreach (var url in imageUrls)
            {
                urlArray.Add(url);
            }
            output["images"] = urlArray;
        }

        if (transcription != null)
        {
            output["transcription"] = transcription;
        }

        // Add metadata
        output["_metadata"] = new JsonObject
        {
            ["topic"] = record.Topic,
            ["partition"] = record.Partition,
            ["offset"] = record.Offset,
            ["timestamp"] = record.Timestamp.ToString("O"),
            ["deployment"] = _deploymentId,
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
