using System.Globalization;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Connect;
using OpenAI;
using OpenAI.Chat;

namespace Kuestenlogik.Surgewave.Connector.OpenAI;

/// <summary>
/// Transform task that calls OpenAI Chat Completions and emits responses to an output topic.
/// Input: raw text or JSON with configurable input field.
/// Output: response text (or JSON with input + response).
/// Creates its own producer if Context.Producer is not available (standalone mode).
/// </summary>
public sealed class OpenAITransformTask : SinkTask
{
    public override string Version => "1.0.0";

    private ChatClient? _chatClient;
    private SurgewaveNativeClient? _ownProducer;
    private string _producerHost = "localhost";
    private int _producerPort = 9092;
    private int _outputPartitionCount;
    private long _roundRobin = -1;
    private string _outputTopic = "";
    private string _systemPrompt = "";
    private string _inputField = "";
    private string _outputFormat = "text";
    private int _maxTokens = 1024;
    private float _temperature = 0.7f;

    public override void Start(IDictionary<string, string> config)
    {
        var apiKey = config.GetValueOrDefault(OpenAIConnectorConfig.ApiKeyConfig, "");
        if (string.IsNullOrEmpty(apiKey))
            apiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY") ?? "";

        if (string.IsNullOrEmpty(apiKey))
            throw new ArgumentException("OpenAI API key required (config or OPENAI_API_KEY env var)");

        _outputTopic = config.GetValueOrDefault(OpenAIConnectorConfig.OutputTopicConfig, "") ?? "";
        if (string.IsNullOrEmpty(_outputTopic))
            throw new ArgumentException("output.topic is required");

        var model = config.GetValueOrDefault(OpenAIConnectorConfig.CompletionsModelConfig, "") ?? "";
        _systemPrompt = config.GetValueOrDefault(OpenAIConnectorConfig.SystemPromptConfig, "") ?? "You are a helpful assistant.";
        _inputField = config.GetValueOrDefault(OpenAIConnectorConfig.InputFieldConfig, "") ?? "";
        _outputFormat = config.GetValueOrDefault(OpenAIConnectorConfig.OutputFormatConfig, "") ?? "text";

        if (config.TryGetValue(OpenAIConnectorConfig.MaxTokensConfig, out var maxStr)
            && int.TryParse(maxStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out var max))
            _maxTokens = max;
        if (config.TryGetValue(OpenAIConnectorConfig.TemperatureConfig, out var tempStr)
            && float.TryParse(tempStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var temp))
            _temperature = temp;

        var baseUrl = config.GetValueOrDefault(OpenAIConnectorConfig.BaseUrlConfig, "") ?? "";
        var clientOptions = !string.IsNullOrEmpty(baseUrl)
            ? new OpenAIClientOptions { Endpoint = new Uri(baseUrl) }
            : new OpenAIClientOptions();

        var credential = new System.ClientModel.ApiKeyCredential(apiKey);
        var client = new OpenAIClient(credential, clientOptions);
        if (string.IsNullOrEmpty(model)) model = OpenAIConnectorConfig.DefaultCompletionsModel;
        _chatClient = client.GetChatClient(model);

        // Remember where an own producer would connect to in standalone mode
        // (when Context.Producer is not injected). The connection itself is opened
        // lazily on the first put so Start() never blocks on I/O.
        var bootstrapServers = config.GetValueOrDefault("bootstrap.servers", "") ?? "";
        if (string.IsNullOrEmpty(bootstrapServers)) bootstrapServers = "localhost:9092";
        var parts = bootstrapServers.Split(':');
        _producerHost = parts[0];
        _producerPort = parts.Length > 1 && int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var p)
            ? p
            : 9092;
    }

    public override void Stop()
    {
        _chatClient = null;
        DisposeProducer();
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeProducer();
        }
        base.Dispose(disposing);
    }

    private void DisposeProducer()
    {
        if (_ownProducer is null)
            return;

        _ownProducer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _ownProducer = null;
        _outputPartitionCount = 0;
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_chatClient is null) return;

        foreach (var record in records)
        {
            try
            {
                var inputText = ExtractInput(record);
                if (string.IsNullOrWhiteSpace(inputText))
                {
                    // Poison record: nothing to send to the model - skip it, but stay visible
                    Context?.RaiseError?.Invoke(new InvalidOperationException(FormattableString.Invariant(
                        $"Skipping record {record.Topic}:{record.Partition}:{record.Offset}: no input text")));
                    continue;
                }

                var messages = new List<ChatMessage>();
                if (!string.IsNullOrEmpty(_systemPrompt))
                    messages.Add(ChatMessage.CreateSystemMessage(_systemPrompt));
                messages.Add(ChatMessage.CreateUserMessage(inputText));

                var options = new ChatCompletionOptions
                {
                    MaxOutputTokenCount = _maxTokens,
                    Temperature = _temperature
                };

                var response = await _chatClient!.CompleteChatAsync(messages, options, cancellationToken);
                var responseText = response.Value.Content[0].Text ?? "";

                var outputBytes = FormatOutput(inputText, responseText, record);

                if (Context?.Producer != null)
                {
                    await Context.Producer.ProduceAsync(
                        _outputTopic,
                        record.Key,
                        outputBytes,
                        cancellationToken);
                }
                else
                {
                    var producer = await EnsureProducerAsync(cancellationToken);
                    var partition = await ResolvePartitionAsync(producer, record.Key, cancellationToken);

                    await producer.Messaging.SendAsync(
                        _outputTopic, partition,
                        record.Key,
                        outputBytes,
                        cancellationToken);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The response was never produced - surface it and fail the batch so the
                // runner can retry or DLQ instead of committing the offset silently.
                Context?.RaiseError?.Invoke(ex);
                throw;
            }
        }
    }

    private async Task<SurgewaveNativeClient> EnsureProducerAsync(CancellationToken cancellationToken)
    {
        var producer = _ownProducer ??= new SurgewaveNativeClient(_producerHost, _producerPort);

        if (!producer.IsConnected)
            await producer.ConnectAsync(cancellationToken);

        return producer;
    }

    /// <summary>
    /// Spreads output across the topic's partitions: keyed records stick to one partition,
    /// unkeyed records go round-robin.
    /// </summary>
    private async Task<int> ResolvePartitionAsync(SurgewaveNativeClient producer, byte[]? key, CancellationToken cancellationToken)
    {
        if (_outputPartitionCount <= 0)
        {
            try
            {
                var description = await producer.Topics.DescribeAsync(_outputTopic, cancellationToken);
                _outputPartitionCount = Math.Max(1, description.PartitionCount);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Layout unknown (e.g. the topic is created on first produce) - stay on partition 0
                Context?.RaiseError?.Invoke(ex);
                return 0;
            }
        }

        if (_outputPartitionCount == 1)
            return 0;

        if (key is { Length: > 0 })
            return (int)(FnvHash(key) % (uint)_outputPartitionCount);

        return (int)(Interlocked.Increment(ref _roundRobin) % _outputPartitionCount);
    }

    private static uint FnvHash(ReadOnlySpan<byte> key)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (var b in key)
            {
                hash = (hash ^ b) * 16777619;
            }
            return hash;
        }
    }

    private string ExtractInput(SinkRecord record)
    {
        if (record.Value is null || record.Value.Length == 0)
            return "";

        var raw = Encoding.UTF8.GetString(record.Value);

        // If no input field configured, use raw value as text
        if (string.IsNullOrEmpty(_inputField))
            return raw;

        // Try to extract field from JSON
        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.TryGetProperty(_inputField, out var field))
                return field.GetString() ?? "";
        }
        catch (JsonException)
        {
            // Not JSON — use raw text
        }

        return raw;
    }

    private byte[] FormatOutput(string input, string response, SinkRecord record)
    {
        if (_outputFormat == "json")
        {
            var json = JsonSerializer.Serialize(new
            {
                input,
                response,
                model = "openai",
                topic = record.Topic,
                timestamp = DateTimeOffset.UtcNow
            });
            return Encoding.UTF8.GetBytes(json);
        }

        // Default: plain text response
        return Encoding.UTF8.GetBytes(response);
    }
}
