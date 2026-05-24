namespace Kuestenlogik.Surgewave.Connector.OpenAI;

using System.ClientModel;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Connect;
using global::OpenAI;
using global::OpenAI.Audio;
using global::OpenAI.Chat;
using global::OpenAI.Embeddings;
using global::OpenAI.Images;
using global::OpenAI.Moderations;

/// <summary>
/// Task that processes records through OpenAI APIs.
/// Supports embeddings, completions, speech (TTS), transcription (Whisper), images (DALL-E), and moderation modes.
/// </summary>
public sealed class OpenAISinkTask : SinkTask
{
    private OpenAIClient? _client;
    private EmbeddingClient? _embeddingClient;
    private ChatClient? _chatClient;
    private AudioClient? _audioClient;
    private ImageClient? _imageClient;
    private ModerationClient? _moderationClient;
    private string _mode = OpenAIConnectorConfig.ModeEmbeddings;
    private string _embeddingsModel = OpenAIConnectorConfig.DefaultEmbeddingsModel;
    private int _embeddingsDimensions;
    private string _completionsModel = OpenAIConnectorConfig.DefaultCompletionsModel;
    private string _systemPrompt = "";
    private int _maxTokens = OpenAIConnectorConfig.DefaultMaxTokens;
    private float _temperature = (float)OpenAIConnectorConfig.DefaultTemperature;
    private string _inputField = OpenAIConnectorConfig.DefaultInputField;
    private string _outputField = OpenAIConnectorConfig.DefaultOutputField;
    private string _webhookUrl = "";
    private bool _includeOriginal = OpenAIConnectorConfig.DefaultIncludeOriginal;
    private int _batchSize = OpenAIConnectorConfig.DefaultBatchSize;
    private int _retryMax = OpenAIConnectorConfig.DefaultRetryMax;
    private int _retryBackoffMs = OpenAIConnectorConfig.DefaultRetryBackoffMs;

    // Speech config
    private string _speechModel = OpenAIConnectorConfig.DefaultSpeechModel;
    private string _speechVoice = OpenAIConnectorConfig.DefaultSpeechVoice;
    private string _speechFormat = OpenAIConnectorConfig.DefaultSpeechFormat;
    private float _speechSpeed = (float)OpenAIConnectorConfig.DefaultSpeechSpeed;

    // Transcription config
    private string _transcriptionModel = OpenAIConnectorConfig.DefaultTranscriptionModel;
    private string? _transcriptionLanguage;
    private string? _transcriptionPrompt;
    private string _transcriptionFormat = OpenAIConnectorConfig.DefaultTranscriptionFormat;
    private string? _transcriptionTimestamps;

    // Images config
    private string _imagesModel = OpenAIConnectorConfig.DefaultImagesModel;
    private string _imagesSize = OpenAIConnectorConfig.DefaultImagesSize;
    private string _imagesQuality = OpenAIConnectorConfig.DefaultImagesQuality;
    private string _imagesStyle = OpenAIConnectorConfig.DefaultImagesStyle;
    private int _imagesCount = OpenAIConnectorConfig.DefaultImagesCount;

    // Moderation config
    private string _moderationModel = OpenAIConnectorConfig.DefaultModerationModel;

    private readonly List<SinkRecord> _buffer = [];
    private HttpClient? _httpClient;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        _client = CreateClient(config);

        _mode = GetConfig(config, OpenAIConnectorConfig.ModeConfig, OpenAIConnectorConfig.ModeEmbeddings);
        _embeddingsModel = GetConfig(config, OpenAIConnectorConfig.EmbeddingsModelConfig, OpenAIConnectorConfig.DefaultEmbeddingsModel);
        _embeddingsDimensions = int.Parse(GetConfig(config, OpenAIConnectorConfig.EmbeddingsDimensionsConfig, "0"));
        _completionsModel = GetConfig(config, OpenAIConnectorConfig.CompletionsModelConfig, OpenAIConnectorConfig.DefaultCompletionsModel);
        _systemPrompt = GetConfig(config, OpenAIConnectorConfig.SystemPromptConfig, "");
        _maxTokens = int.Parse(GetConfig(config, OpenAIConnectorConfig.MaxTokensConfig, OpenAIConnectorConfig.DefaultMaxTokens.ToString()));
        _temperature = float.Parse(GetConfig(config, OpenAIConnectorConfig.TemperatureConfig, OpenAIConnectorConfig.DefaultTemperature.ToString()));
        _inputField = GetConfig(config, OpenAIConnectorConfig.InputFieldConfig, OpenAIConnectorConfig.DefaultInputField);
        _outputField = GetConfig(config, OpenAIConnectorConfig.OutputFieldConfig, OpenAIConnectorConfig.DefaultOutputField);
        _webhookUrl = GetConfig(config, "webhook.url", "");
        _includeOriginal = bool.Parse(GetConfig(config, OpenAIConnectorConfig.IncludeOriginalConfig, OpenAIConnectorConfig.DefaultIncludeOriginal.ToString()));
        _batchSize = int.Parse(GetConfig(config, OpenAIConnectorConfig.BatchSizeConfig, OpenAIConnectorConfig.DefaultBatchSize.ToString()));
        _retryMax = int.Parse(GetConfig(config, OpenAIConnectorConfig.RetryMaxConfig, OpenAIConnectorConfig.DefaultRetryMax.ToString()));
        _retryBackoffMs = int.Parse(GetConfig(config, OpenAIConnectorConfig.RetryBackoffMsConfig, OpenAIConnectorConfig.DefaultRetryBackoffMs.ToString()));

        // Speech config
        _speechModel = GetConfig(config, OpenAIConnectorConfig.SpeechModelConfig, OpenAIConnectorConfig.DefaultSpeechModel);
        _speechVoice = GetConfig(config, OpenAIConnectorConfig.SpeechVoiceConfig, OpenAIConnectorConfig.DefaultSpeechVoice);
        _speechFormat = GetConfig(config, OpenAIConnectorConfig.SpeechFormatConfig, OpenAIConnectorConfig.DefaultSpeechFormat);
        _speechSpeed = float.Parse(GetConfig(config, OpenAIConnectorConfig.SpeechSpeedConfig, OpenAIConnectorConfig.DefaultSpeechSpeed.ToString()));

        // Transcription config
        _transcriptionModel = GetConfig(config, OpenAIConnectorConfig.TranscriptionModelConfig, OpenAIConnectorConfig.DefaultTranscriptionModel);
        _transcriptionLanguage = config.TryGetValue(OpenAIConnectorConfig.TranscriptionLanguageConfig, out var lang) ? lang : null;
        _transcriptionPrompt = config.TryGetValue(OpenAIConnectorConfig.TranscriptionPromptConfig, out var prompt) ? prompt : null;
        _transcriptionFormat = GetConfig(config, OpenAIConnectorConfig.TranscriptionFormatConfig, OpenAIConnectorConfig.DefaultTranscriptionFormat);
        _transcriptionTimestamps = config.TryGetValue(OpenAIConnectorConfig.TranscriptionTimestampsConfig, out var ts) ? ts : null;

        // Images config
        _imagesModel = GetConfig(config, OpenAIConnectorConfig.ImagesModelConfig, OpenAIConnectorConfig.DefaultImagesModel);
        _imagesSize = GetConfig(config, OpenAIConnectorConfig.ImagesSizeConfig, OpenAIConnectorConfig.DefaultImagesSize);
        _imagesQuality = GetConfig(config, OpenAIConnectorConfig.ImagesQualityConfig, OpenAIConnectorConfig.DefaultImagesQuality);
        _imagesStyle = GetConfig(config, OpenAIConnectorConfig.ImagesStyleConfig, OpenAIConnectorConfig.DefaultImagesStyle);
        _imagesCount = int.Parse(GetConfig(config, OpenAIConnectorConfig.ImagesCountConfig, OpenAIConnectorConfig.DefaultImagesCount.ToString()));

        // Moderation config
        _moderationModel = GetConfig(config, OpenAIConnectorConfig.ModerationModelConfig, OpenAIConnectorConfig.DefaultModerationModel);

        // Initialize the appropriate client based on mode
        switch (_mode)
        {
            case OpenAIConnectorConfig.ModeEmbeddings:
                _embeddingClient = _client.GetEmbeddingClient(_embeddingsModel);
                break;
            case OpenAIConnectorConfig.ModeCompletions:
                _chatClient = _client.GetChatClient(_completionsModel);
                break;
            case OpenAIConnectorConfig.ModeSpeech:
            case OpenAIConnectorConfig.ModeTranscription:
                _audioClient = _client.GetAudioClient(_mode == OpenAIConnectorConfig.ModeSpeech ? _speechModel : _transcriptionModel);
                break;
            case OpenAIConnectorConfig.ModeImages:
                _imageClient = _client.GetImageClient(_imagesModel);
                break;
            case OpenAIConnectorConfig.ModeModeration:
                _moderationClient = _client.GetModerationClient(_moderationModel);
                break;
        }

        // HTTP client for webhook
        if (!string.IsNullOrEmpty(_webhookUrl))
        {
            _httpClient = new HttpClient();
        }
    }

    private static OpenAIClient CreateClient(IDictionary<string, string> config)
    {
        var apiKey = config.TryGetValue(OpenAIConnectorConfig.ApiKeyConfig, out var key) && !string.IsNullOrEmpty(key)
            ? key
            : Environment.GetEnvironmentVariable("OPENAI_API_KEY")
              ?? throw new InvalidOperationException("OpenAI API key not found");

        var options = new OpenAIClientOptions();

        if (config.TryGetValue(OpenAIConnectorConfig.BaseUrlConfig, out var baseUrl) && !string.IsNullOrEmpty(baseUrl))
        {
            options.Endpoint = new Uri(baseUrl);
        }

        if (config.TryGetValue(OpenAIConnectorConfig.OrganizationConfig, out var org) && !string.IsNullOrEmpty(org))
        {
            options.OrganizationId = org;
        }

        if (config.TryGetValue(OpenAIConnectorConfig.ProjectConfig, out var project) && !string.IsNullOrEmpty(project))
        {
            options.ProjectId = project;
        }

        return new OpenAIClient(new ApiKeyCredential(apiKey), options);
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

        switch (_mode)
        {
            case OpenAIConnectorConfig.ModeEmbeddings:
                await ProcessEmbeddingsAsync(batch, cancellationToken);
                break;
            case OpenAIConnectorConfig.ModeCompletions:
                await ProcessCompletionsAsync(batch, cancellationToken);
                break;
            case OpenAIConnectorConfig.ModeSpeech:
                await ProcessSpeechAsync(batch, cancellationToken);
                break;
            case OpenAIConnectorConfig.ModeTranscription:
                await ProcessTranscriptionAsync(batch, cancellationToken);
                break;
            case OpenAIConnectorConfig.ModeImages:
                await ProcessImagesAsync(batch, cancellationToken);
                break;
            case OpenAIConnectorConfig.ModeModeration:
                await ProcessModerationAsync(batch, cancellationToken);
                break;
        }
    }

    private async Task ProcessEmbeddingsAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_embeddingClient == null)
            return;

        // Extract text from each record
        var texts = new List<string>();
        var validRecords = new List<(SinkRecord Record, JsonNode? Original, string RawValue)>();

        foreach (var record in records)
        {
            var rawValue = Encoding.UTF8.GetString(record.Value);
            try
            {
                var json = JsonNode.Parse(rawValue);
                var text = json?[_inputField]?.GetValue<string>();

                if (!string.IsNullOrEmpty(text))
                {
                    texts.Add(text);
                    validRecords.Add((record, json, rawValue));
                }
            }
            catch (JsonException)
            {
                // Use raw value if not JSON
                if (!string.IsNullOrEmpty(rawValue))
                {
                    texts.Add(rawValue);
                    validRecords.Add((record, null, rawValue));
                }
            }
        }

        if (texts.Count == 0)
            return;

        // Call OpenAI embeddings API with retry
        OpenAIEmbeddingCollection? embeddings = null;
        for (int attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                var options = new EmbeddingGenerationOptions();
                if (_embeddingsDimensions > 0)
                {
                    options.Dimensions = _embeddingsDimensions;
                }

                embeddings = await _embeddingClient.GenerateEmbeddingsAsync(texts, options, cancellationToken);
                break;
            }
            catch (ClientResultException) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }

        if (embeddings == null)
            return;

        // Process output
        for (int i = 0; i < validRecords.Count && i < embeddings.Count; i++)
        {
            var (record, original, rawValue) = validRecords[i];
            var embedding = embeddings[i].ToFloats().ToArray();

            var output = CreateOutputJson(original, embedding, rawValue);
            await SendOutputAsync(output, record, cancellationToken);
        }
    }

    private async Task ProcessCompletionsAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_chatClient == null)
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

            // Call OpenAI chat API with retry
            string? completion = null;
            for (int attempt = 0; attempt <= _retryMax; attempt++)
            {
                try
                {
                    var messages = new List<ChatMessage>
                    {
                        ChatMessage.CreateSystemMessage(_systemPrompt),
                        ChatMessage.CreateUserMessage(inputText)
                    };

                    var options = new ChatCompletionOptions
                    {
                        MaxOutputTokenCount = _maxTokens,
                        Temperature = _temperature
                    };

                    var response = await _chatClient.CompleteChatAsync(messages, options, cancellationToken);
                    completion = response.Value.Content[0].Text;
                    break;
                }
                catch (ClientResultException) when (attempt < _retryMax)
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

    private async Task ProcessSpeechAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_audioClient == null)
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

            // Call OpenAI TTS API with retry
            byte[]? audioBytes = null;
            for (int attempt = 0; attempt <= _retryMax; attempt++)
            {
                try
                {
                    var voice = _speechVoice.ToLowerInvariant() switch
                    {
                        "echo" => GeneratedSpeechVoice.Echo,
                        "fable" => GeneratedSpeechVoice.Fable,
                        "onyx" => GeneratedSpeechVoice.Onyx,
                        "nova" => GeneratedSpeechVoice.Nova,
                        "shimmer" => GeneratedSpeechVoice.Shimmer,
                        _ => GeneratedSpeechVoice.Alloy
                    };

                    var format = _speechFormat.ToLowerInvariant() switch
                    {
                        "opus" => GeneratedSpeechFormat.Opus,
                        "aac" => GeneratedSpeechFormat.Aac,
                        "flac" => GeneratedSpeechFormat.Flac,
                        "wav" => GeneratedSpeechFormat.Wav,
                        "pcm" => GeneratedSpeechFormat.Pcm,
                        _ => GeneratedSpeechFormat.Mp3
                    };

                    var options = new SpeechGenerationOptions
                    {
                        SpeedRatio = _speechSpeed,
                        ResponseFormat = format
                    };

                    var response = await _audioClient.GenerateSpeechAsync(inputText, voice, options, cancellationToken);
                    audioBytes = response.Value.ToArray();
                    break;
                }
                catch (ClientResultException) when (attempt < _retryMax)
                {
                    await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                }
            }

            if (audioBytes == null)
                continue;

            var output = CreateOutputJson(original, Convert.ToBase64String(audioBytes), rawValue);
            output["audio_format"] = _speechFormat;
            await SendOutputAsync(output, record, cancellationToken);
        }
    }

    private async Task ProcessTranscriptionAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_audioClient == null)
            return;

        foreach (var record in records)
        {
            // Record value should contain audio data (base64 encoded or raw bytes)
            byte[] audioBytes;
            var rawValue = Encoding.UTF8.GetString(record.Value);
            JsonNode? original = null;

            try
            {
                original = JsonNode.Parse(rawValue);
                var audioBase64 = original?[_inputField]?.GetValue<string>();
                if (string.IsNullOrEmpty(audioBase64))
                    continue;
                audioBytes = Convert.FromBase64String(audioBase64);
            }
            catch (JsonException)
            {
                // Try to use raw value as base64
                try
                {
                    audioBytes = Convert.FromBase64String(rawValue);
                }
                catch
                {
                    // Assume raw audio bytes
                    audioBytes = record.Value;
                }
            }

            if (audioBytes.Length == 0)
                continue;

            // Call OpenAI Whisper API with retry
            string? transcription = null;
            for (int attempt = 0; attempt <= _retryMax; attempt++)
            {
                try
                {
                    using var stream = new MemoryStream(audioBytes);
                    var options = new AudioTranscriptionOptions
                    {
                        ResponseFormat = _transcriptionFormat.ToLowerInvariant() switch
                        {
                            "text" => AudioTranscriptionFormat.Text,
                            "srt" => AudioTranscriptionFormat.Srt,
                            "vtt" => AudioTranscriptionFormat.Vtt,
                            "verbose_json" => AudioTranscriptionFormat.Verbose,
                            _ => AudioTranscriptionFormat.Simple
                        }
                    };

                    if (!string.IsNullOrEmpty(_transcriptionLanguage))
                        options.Language = _transcriptionLanguage;
                    if (!string.IsNullOrEmpty(_transcriptionPrompt))
                        options.Prompt = _transcriptionPrompt;
                    if (_transcriptionTimestamps == OpenAIConnectorConfig.TranscriptionTimestampsWord)
                        options.TimestampGranularities = AudioTimestampGranularities.Word;
                    else if (_transcriptionTimestamps == OpenAIConnectorConfig.TranscriptionTimestampsSegment)
                        options.TimestampGranularities = AudioTimestampGranularities.Segment;

                    var response = await _audioClient.TranscribeAudioAsync(stream, "audio.wav", options, cancellationToken);
                    transcription = response.Value.Text;
                    break;
                }
                catch (ClientResultException) when (attempt < _retryMax)
                {
                    await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                }
            }

            if (transcription == null)
                continue;

            var output = CreateOutputJson(original, transcription, rawValue);
            await SendOutputAsync(output, record, cancellationToken);
        }
    }

    private async Task ProcessImagesAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_imageClient == null)
            return;

        foreach (var record in records)
        {
            var rawValue = Encoding.UTF8.GetString(record.Value);
            JsonNode? original = null;
            string? prompt = null;

            try
            {
                original = JsonNode.Parse(rawValue);
                prompt = original?[_inputField]?.GetValue<string>();
            }
            catch (JsonException)
            {
                prompt = rawValue;
            }

            if (string.IsNullOrEmpty(prompt))
                continue;

            // Call OpenAI DALL-E API with retry
            List<string>? imageUrls = null;
            for (int attempt = 0; attempt <= _retryMax; attempt++)
            {
                try
                {
                    var (width, height) = ParseImageSize(_imagesSize);
                    var options = new ImageGenerationOptions
                    {
                        Size = new GeneratedImageSize(width, height),
                        Quality = string.Equals(_imagesQuality, "hd", StringComparison.OrdinalIgnoreCase) ? GeneratedImageQuality.High : GeneratedImageQuality.Standard,
                        Style = string.Equals(_imagesStyle, "natural", StringComparison.OrdinalIgnoreCase) ? GeneratedImageStyle.Natural : GeneratedImageStyle.Vivid,
                        ResponseFormat = GeneratedImageFormat.Uri
                    };

                    var response = await _imageClient.GenerateImagesAsync(prompt, _imagesCount, options, cancellationToken);
                    imageUrls = response.Value.Select(img => img.ImageUri?.ToString() ?? "").ToList();
                    break;
                }
                catch (ClientResultException) when (attempt < _retryMax)
                {
                    await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
                }
            }

            if (imageUrls == null || imageUrls.Count == 0)
                continue;

            var output = CreateOutputJson(original, imageUrls.Count == 1 ? imageUrls[0] : imageUrls, rawValue);
            await SendOutputAsync(output, record, cancellationToken);
        }
    }

    private static (int width, int height) ParseImageSize(string size)
    {
        var parts = size.Split('x');
        if (parts.Length == 2 && int.TryParse(parts[0], out var w) && int.TryParse(parts[1], out var h))
            return (w, h);
        return (1024, 1024);
    }

    private async Task ProcessModerationAsync(List<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_moderationClient == null)
            return;

        // Extract texts from all records
        var texts = new List<string>();
        var validRecords = new List<(SinkRecord Record, JsonNode? Original, string RawValue)>();

        foreach (var record in records)
        {
            var rawValue = Encoding.UTF8.GetString(record.Value);
            try
            {
                var json = JsonNode.Parse(rawValue);
                var text = json?[_inputField]?.GetValue<string>();

                if (!string.IsNullOrEmpty(text))
                {
                    texts.Add(text);
                    validRecords.Add((record, json, rawValue));
                }
            }
            catch (JsonException)
            {
                if (!string.IsNullOrEmpty(rawValue))
                {
                    texts.Add(rawValue);
                    validRecords.Add((record, null, rawValue));
                }
            }
        }

        if (texts.Count == 0)
            return;

        // Call OpenAI moderation API with retry
        ModerationResultCollection? results = null;
        for (int attempt = 0; attempt <= _retryMax; attempt++)
        {
            try
            {
                results = await _moderationClient.ClassifyTextAsync(texts, cancellationToken);
                break;
            }
            catch (ClientResultException) when (attempt < _retryMax)
            {
                await Task.Delay(_retryBackoffMs * (attempt + 1), cancellationToken);
            }
        }

        if (results == null)
            return;

        // Process output
        for (int i = 0; i < validRecords.Count && i < results.Count; i++)
        {
            var (record, original, rawValue) = validRecords[i];
            var result = results[i];

            // Build moderation result using available properties
            var moderationResult = new JsonObject
            {
                ["flagged"] = result.Flagged
            };

            // Add category flags
            var categories = new JsonObject();
            var categoryScores = new JsonObject();

            // Use the Hate/HateThreatening and other properties from ModerationCategories
            categories["hate"] = result.Hate.Flagged;
            categoryScores["hate"] = result.Hate.Score;

            categories["hate_threatening"] = result.HateThreatening.Flagged;
            categoryScores["hate_threatening"] = result.HateThreatening.Score;

            categories["harassment"] = result.Harassment.Flagged;
            categoryScores["harassment"] = result.Harassment.Score;

            categories["harassment_threatening"] = result.HarassmentThreatening.Flagged;
            categoryScores["harassment_threatening"] = result.HarassmentThreatening.Score;

            categories["self_harm"] = result.SelfHarm.Flagged;
            categoryScores["self_harm"] = result.SelfHarm.Score;

            categories["self_harm_intent"] = result.SelfHarmIntent.Flagged;
            categoryScores["self_harm_intent"] = result.SelfHarmIntent.Score;

            categories["self_harm_instructions"] = result.SelfHarmInstructions.Flagged;
            categoryScores["self_harm_instructions"] = result.SelfHarmInstructions.Score;

            categories["sexual"] = result.Sexual.Flagged;
            categoryScores["sexual"] = result.Sexual.Score;

            categories["sexual_minors"] = result.SexualMinors.Flagged;
            categoryScores["sexual_minors"] = result.SexualMinors.Score;

            categories["violence"] = result.Violence.Flagged;
            categoryScores["violence"] = result.Violence.Score;

            categories["violence_graphic"] = result.ViolenceGraphic.Flagged;
            categoryScores["violence_graphic"] = result.ViolenceGraphic.Score;

            moderationResult["categories"] = categories;
            moderationResult["category_scores"] = categoryScores;

            var output = CreateOutputJson(original, moderationResult, rawValue);
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
        if (_httpClient != null && !string.IsNullOrEmpty(_webhookUrl))
        {
            try
            {
                using var content = new StringContent(json, Encoding.UTF8, "application/json");
                await _httpClient.PostAsync(new Uri(_webhookUrl), content, cancellationToken);
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
            Console.WriteLine($"[OpenAI] {sourceRecord.Topic}:{sourceRecord.Partition}:{sourceRecord.Offset} -> {json[..Math.Min(200, json.Length)]}...");
        }
    }

    public override void Stop()
    {
        _buffer.Clear();
        _embeddingClient = null;
        _chatClient = null;
        _audioClient = null;
        _imageClient = null;
        _moderationClient = null;
        _client = null;
        _httpClient?.Dispose();
        _httpClient = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _httpClient?.Dispose();
            _httpClient = null;
        }
        base.Dispose(disposing);
    }

    private static string GetConfig(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) ? value : defaultValue;
}
