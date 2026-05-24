namespace Kuestenlogik.Surgewave.Connector.OpenAI.Tests;

using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class OpenAISinkTaskTests
{
    [Fact]
    public void OpenAISinkTask_HasCorrectVersion()
    {
        using var task = new OpenAISinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void OpenAISinkTask_Start_InitializesWithValidConfig()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.ModeConfig] = OpenAIConnectorConfig.ModeEmbeddings,
            [OpenAIConnectorConfig.InputFieldConfig] = "text",
            [OpenAIConnectorConfig.OutputFieldConfig] = "embedding"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OpenAISinkTask_Start_InitializesCompletionsMode()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.ModeConfig] = OpenAIConnectorConfig.ModeCompletions,
            [OpenAIConnectorConfig.SystemPromptConfig] = "Test system prompt",
            [OpenAIConnectorConfig.InputFieldConfig] = "content",
            [OpenAIConnectorConfig.OutputFieldConfig] = "summary"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OpenAISinkTask_Start_UsesEnvironmentApiKey()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var originalApiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("OPENAI_API_KEY", "test-env-api-key");

            var config = new Dictionary<string, string>
            {
                [OpenAIConnectorConfig.TopicsConfig] = "test-topic"
            };

            // Should not throw - API key from environment
            task.Start(config);
            task.Stop();
        }
        finally
        {
            Environment.SetEnvironmentVariable("OPENAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void OpenAISinkTask_Start_ThrowsWithoutApiKey()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var originalApiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("OPENAI_API_KEY", null);

            var config = new Dictionary<string, string>
            {
                [OpenAIConnectorConfig.TopicsConfig] = "test-topic"
            };

            Assert.Throws<InvalidOperationException>(() => task.Start(config));
        }
        finally
        {
            Environment.SetEnvironmentVariable("OPENAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public async Task OpenAISinkTask_PutAsync_BuffersRecords()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.BatchSizeConfig] = "100" // High batch size so it won't flush
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
    public async Task OpenAISinkTask_FlushAsync_HandlesEmptyBuffer()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);

        // Should not throw with empty buffer
        await task.FlushAsync(new Dictionary<TopicPartition, long>(), CancellationToken.None);

        task.Stop();
    }

    [Fact]
    public void OpenAISinkTask_Start_ParsesBatchConfig()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.BatchSizeConfig] = "50",
            [OpenAIConnectorConfig.RetryMaxConfig] = "5",
            [OpenAIConnectorConfig.RetryBackoffMsConfig] = "2000"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OpenAISinkTask_Start_ParsesEmbeddingsConfig()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.ModeConfig] = OpenAIConnectorConfig.ModeEmbeddings,
            [OpenAIConnectorConfig.EmbeddingsModelConfig] = "text-embedding-3-large",
            [OpenAIConnectorConfig.EmbeddingsDimensionsConfig] = "3072",
            [OpenAIConnectorConfig.InputFieldConfig] = "content",
            [OpenAIConnectorConfig.OutputFieldConfig] = "vector"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OpenAISinkTask_Start_ParsesCompletionsConfig()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.ModeConfig] = OpenAIConnectorConfig.ModeCompletions,
            [OpenAIConnectorConfig.CompletionsModelConfig] = "gpt-4o",
            [OpenAIConnectorConfig.SystemPromptConfig] = "Summarize the following text:",
            [OpenAIConnectorConfig.MaxTokensConfig] = "500",
            [OpenAIConnectorConfig.TemperatureConfig] = "0.5"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OpenAISinkTask_Start_ParsesWebhookUrl()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            ["webhook.url"] = "https://example.com/webhook"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OpenAISinkTask_Start_ParsesCustomBaseUrl()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.BaseUrlConfig] = "https://api.azure.com/openai"
        };

        // Should not throw - custom base URL for Azure OpenAI
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void OpenAISinkTask_Stop_ClearsBuffer()
    {
        using var task = new OpenAISinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic"
        };

        task.Start(config);
        task.Stop();

        // Multiple stops should be safe
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
