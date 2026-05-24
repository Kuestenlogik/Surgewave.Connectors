namespace Kuestenlogik.Surgewave.Connector.Ollama.Tests;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class OllamaSinkTaskTests
{
    [Fact]
    public void OllamaSinkTask_HasCorrectVersion()
    {
        using var task = new OllamaSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void OllamaSinkTask_Start_InitializesWithValidConfig()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.BaseUrlConfig] = "http://localhost:11434"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_InitializesEmbeddingsMode()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeEmbeddings,
            [OllamaConnectorConfig.EmbeddingsModelConfig] = "nomic-embed-text",
            [OllamaConnectorConfig.InputFieldConfig] = "text",
            [OllamaConnectorConfig.OutputFieldConfig] = "embedding"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_InitializesCompletionsMode()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeCompletions,
            [OllamaConnectorConfig.CompletionsModelConfig] = "llama3",
            [OllamaConnectorConfig.SystemPromptConfig] = "Summarize the following text:",
            [OllamaConnectorConfig.InputFieldConfig] = "content",
            [OllamaConnectorConfig.OutputFieldConfig] = "summary"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_InitializesWithDefaultBaseUrl()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic"
            // No base URL - should use default
        };

        // Should not throw - uses default localhost:11434
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_InitializesWithCustomBaseUrl()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.BaseUrlConfig] = "http://remote-server:11434"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public async Task OllamaSinkTask_PutAsync_BuffersRecords()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.BatchSizeConfig] = "100" // High batch size so it won't flush
        };

        task.Start(config);

        var records = new List<SinkRecord>
        {
            CreateSinkRecord("test-topic", 0, 0, """{"text": "Hello world"}"""),
            CreateSinkRecord("test-topic", 0, 1, """{"text": "Test message"}""")
        };

        // Should not throw - just buffers without calling API
        await task.PutAsync(records, CancellationToken.None);

        task.Stop();
    }

    [Fact]
    public async Task OllamaSinkTask_FlushAsync_HandlesEmptyBuffer()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);

        // Should not throw with empty buffer
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_ParsesBatchConfig()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.BatchSizeConfig] = "50",
            [OllamaConnectorConfig.RetryMaxConfig] = "5",
            [OllamaConnectorConfig.RetryBackoffMsConfig] = "2000"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_ParsesEmbeddingsConfig()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeEmbeddings,
            [OllamaConnectorConfig.EmbeddingsModelConfig] = "mxbai-embed-large",
            [OllamaConnectorConfig.InputFieldConfig] = "content",
            [OllamaConnectorConfig.OutputFieldConfig] = "vector"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_ParsesCompletionsConfig()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeCompletions,
            [OllamaConnectorConfig.CompletionsModelConfig] = "mistral",
            [OllamaConnectorConfig.SystemPromptConfig] = "Analyze this text:",
            [OllamaConnectorConfig.MaxTokensConfig] = "500",
            [OllamaConnectorConfig.TemperatureConfig] = "0.5"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_ParsesWebhookUrl()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.WebhookUrlConfig] = "https://example.com/webhook"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_ParsesKeepAliveConfig()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.KeepAliveConfig] = "10m"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Start_ParsesIncludeOriginalConfig()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.IncludeOriginalConfig] = "false"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Stop_ClearsBuffer()
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);
        task.Stop();

        // Multiple stops should be safe
        task.Stop();
    }

    [Fact]
    public void OllamaSinkTask_Dispose_DisposesResources()
    {
        var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.WebhookUrlConfig] = "https://example.com/webhook"
        };

        task.Start(config);
        task.Dispose();

        // Multiple disposes should be safe
        task.Dispose();
    }

    [Theory]
    [InlineData("llama3")]
    [InlineData("mistral")]
    [InlineData("qwen2")]
    [InlineData("gemma2")]
    public void OllamaSinkTask_Start_AcceptsVariousCompletionModels(string model)
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeCompletions,
            [OllamaConnectorConfig.CompletionsModelConfig] = model
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Theory]
    [InlineData("nomic-embed-text")]
    [InlineData("mxbai-embed-large")]
    [InlineData("all-minilm")]
    public void OllamaSinkTask_Start_AcceptsVariousEmbeddingModels(string model)
    {
        using var task = new OllamaSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeEmbeddings,
            [OllamaConnectorConfig.EmbeddingsModelConfig] = model
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }

    private static SinkRecord CreateSinkRecord(string topic, int partition, long offset, string value)
    {
        return new SinkRecord
        {
            Topic = topic,
            Partition = partition,
            Offset = offset,
            Key = null,
            Value = Encoding.UTF8.GetBytes(value),
            Timestamp = DateTimeOffset.UtcNow,
            Headers = new Dictionary<string, byte[]>()
        };
    }
}
