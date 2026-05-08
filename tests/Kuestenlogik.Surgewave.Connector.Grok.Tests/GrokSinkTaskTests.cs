namespace Kuestenlogik.Surgewave.Connector.Grok.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class GrokSinkTaskTests
{
    [Fact]
    public void GrokSinkTask_HasCorrectVersion()
    {
        using var task = new GrokSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void GrokSinkTask_Start_ThrowsOnMissingApiKey()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        // Clear environment variable if set
        var originalApiKey = Environment.GetEnvironmentVariable("XAI_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", null);

            var config = new Dictionary<string, string>
            {
                [GrokConnectorConfig.TopicsConfig] = "test-topic"
            };

            var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
            Assert.Contains(GrokConnectorConfig.ApiKeyConfig, ex.Message);
        }
        finally
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void GrokSinkTask_Start_AcceptsValidConfig()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void GrokSinkTask_Start_AcceptsApiKeyFromEnvironment()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var originalApiKey = Environment.GetEnvironmentVariable("XAI_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", "test-env-api-key");

            var config = new Dictionary<string, string>
            {
                [GrokConnectorConfig.TopicsConfig] = "test-topic",
                [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
            };

            // Should not throw - API key from environment
            task.Start(config);
            task.Stop();
        }
        finally
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void GrokSinkTask_Start_AcceptsCustomBaseUrl()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt",
            [GrokConnectorConfig.BaseUrlConfig] = "https://custom.api.x.ai/v1"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void GrokSinkTask_Start_AppliesConfigValues()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt",
            [GrokConnectorConfig.ModelConfig] = "grok-3-mini",
            [GrokConnectorConfig.MaxTokensConfig] = "2048",
            [GrokConnectorConfig.TemperatureConfig] = "0.5",
            [GrokConnectorConfig.TopPConfig] = "0.9",
            [GrokConnectorConfig.BatchSizeConfig] = "20",
            [GrokConnectorConfig.BatchTimeoutMsConfig] = "3000",
            [GrokConnectorConfig.RetryMaxConfig] = "5",
            [GrokConnectorConfig.RetryBackoffMsConfig] = "2000"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void GrokSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        task.Start(config);
        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void GrokSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        task.Start(config);
        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task GrokSinkTask_PutAsync_HandlesEmptyRecords()
    {
        using var task = new GrokSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        task.Start(config);

        // Should not throw on empty records
        await task.PutAsync(Array.Empty<SinkRecord>());

        task.Stop();
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
