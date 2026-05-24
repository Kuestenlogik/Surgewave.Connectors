namespace Kuestenlogik.Surgewave.Connector.Anthropic.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class AnthropicSinkTaskTests
{
    [Fact]
    public void AnthropicSinkTask_HasCorrectVersion()
    {
        using var task = new AnthropicSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void AnthropicSinkTask_Start_ThrowsOnMissingApiKey()
    {
        using var task = new AnthropicSinkTask();
        task.Initialize(CreateTaskContext());

        // Clear environment variable if set
        var originalApiKey = Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("ANTHROPIC_API_KEY", null);

            var config = new Dictionary<string, string>
            {
                [AnthropicConnectorConfig.TopicsConfig] = "test-topic"
            };

            var ex = Assert.Throws<ArgumentException>(() => task.Start(config));
            Assert.Contains(AnthropicConnectorConfig.ApiKeyConfig, ex.Message);
        }
        finally
        {
            Environment.SetEnvironmentVariable("ANTHROPIC_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void AnthropicSinkTask_Start_AcceptsValidConfig()
    {
        using var task = new AnthropicSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void AnthropicSinkTask_Start_AcceptsApiKeyFromEnvironment()
    {
        using var task = new AnthropicSinkTask();
        task.Initialize(CreateTaskContext());

        var originalApiKey = Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("ANTHROPIC_API_KEY", "test-env-api-key");

            var config = new Dictionary<string, string>
            {
                [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
                [AnthropicConnectorConfig.SystemPromptConfig] = "Test system prompt"
            };

            // Should not throw - API key from environment
            task.Start(config);
            task.Stop();
        }
        finally
        {
            Environment.SetEnvironmentVariable("ANTHROPIC_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void AnthropicSinkTask_Start_AcceptsCustomBaseUrl()
    {
        using var task = new AnthropicSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.SystemPromptConfig] = "Test system prompt",
            [AnthropicConnectorConfig.BaseUrlConfig] = "https://custom.api.anthropic.com"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void AnthropicSinkTask_Start_AppliesConfigValues()
    {
        using var task = new AnthropicSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.SystemPromptConfig] = "Test system prompt",
            [AnthropicConnectorConfig.ModelConfig] = "claude-3-opus-latest",
            [AnthropicConnectorConfig.MaxTokensConfig] = "2048",
            [AnthropicConnectorConfig.TemperatureConfig] = "0.5",
            [AnthropicConnectorConfig.TopPConfig] = "0.9",
            [AnthropicConnectorConfig.TopKConfig] = "50",
            [AnthropicConnectorConfig.BatchSizeConfig] = "20",
            [AnthropicConnectorConfig.BatchTimeoutMsConfig] = "3000",
            [AnthropicConnectorConfig.RetryMaxConfig] = "5",
            [AnthropicConnectorConfig.RetryBackoffMsConfig] = "2000"
        };

        // Should not throw
        task.Start(config);
        task.Stop();
    }

    [Fact]
    public void AnthropicSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new AnthropicSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        task.Start(config);
        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void AnthropicSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new AnthropicSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        task.Start(config);
        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task AnthropicSinkTask_PutAsync_HandlesEmptyRecords()
    {
        using var task = new AnthropicSinkTask();
        task.Initialize(CreateTaskContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.SystemPromptConfig] = "Test system prompt"
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
