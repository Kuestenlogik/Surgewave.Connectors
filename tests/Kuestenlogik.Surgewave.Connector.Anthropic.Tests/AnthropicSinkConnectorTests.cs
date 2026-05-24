namespace Kuestenlogik.Surgewave.Connector.Anthropic.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class AnthropicSinkConnectorTests
{
    [Fact]
    public void AnthropicSinkConnector_HasCorrectVersion()
    {
        using var connector = new AnthropicSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void AnthropicSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new AnthropicSinkConnector();
        Assert.Equal(typeof(AnthropicSinkTask), connector.TaskClass);
    }

    [Fact]
    public void AnthropicSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new AnthropicSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.ApiKeyConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void AnthropicSinkConnector_Config_HasCompletionsKeys()
    {
        using var connector = new AnthropicSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.ModelConfig);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.SystemPromptConfig);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.MaxTokensConfig);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.TemperatureConfig);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.TopPConfig);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.TopKConfig);
    }

    [Fact]
    public void AnthropicSinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new AnthropicSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.OutputFieldConfig);
    }

    [Fact]
    public void AnthropicSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new AnthropicSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == AnthropicConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void AnthropicSinkConnector_Start_ThrowsOnMissingApiKey()
    {
        using var connector = new AnthropicSinkConnector();
        connector.Initialize(CreateContext());

        // Clear environment variable if set
        var originalApiKey = Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("ANTHROPIC_API_KEY", null);

            var config = new Dictionary<string, string>
            {
                [AnthropicConnectorConfig.TopicsConfig] = "test-topic"
            };

            var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
            Assert.Contains(AnthropicConnectorConfig.ApiKeyConfig, ex.Message);
        }
        finally
        {
            Environment.SetEnvironmentVariable("ANTHROPIC_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void AnthropicSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new AnthropicSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(AnthropicConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void AnthropicSinkConnector_Start_ThrowsOnMissingSystemPrompt()
    {
        using var connector = new AnthropicSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.ModeConfig] = AnthropicConnectorConfig.ModeCompletions
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(AnthropicConnectorConfig.SystemPromptConfig, ex.Message);
    }

    [Fact]
    public void AnthropicSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new AnthropicSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void AnthropicSinkConnector_Start_AcceptsValidConfig()
    {
        using var connector = new AnthropicSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.ModeConfig] = AnthropicConnectorConfig.ModeCompletions,
            [AnthropicConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void AnthropicSinkConnector_Start_AcceptsApiKeyFromEnvironment()
    {
        using var connector = new AnthropicSinkConnector();
        connector.Initialize(CreateContext());

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
            connector.Start(config);
            connector.Stop();
        }
        finally
        {
            Environment.SetEnvironmentVariable("ANTHROPIC_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void AnthropicSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new AnthropicSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AnthropicConnectorConfig.ApiKeyConfig] = "test-api-key",
            [AnthropicConnectorConfig.TopicsConfig] = "test-topic",
            [AnthropicConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-api-key", taskConfigs[0][AnthropicConnectorConfig.ApiKeyConfig]);
        Assert.Equal("test-topic", taskConfigs[0][AnthropicConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void AnthropicSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new AnthropicSinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == AnthropicConnectorConfig.ModeConfig);
        Assert.Equal(AnthropicConnectorConfig.ModeCompletions, modeKey.DefaultValue);

        var modelKey = config.Keys.First(k => k.Name == AnthropicConnectorConfig.ModelConfig);
        Assert.Equal(AnthropicConnectorConfig.DefaultModel, modelKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == AnthropicConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)AnthropicConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var maxTokensKey = config.Keys.First(k => k.Name == AnthropicConnectorConfig.MaxTokensConfig);
        Assert.Equal((long)AnthropicConnectorConfig.DefaultMaxTokens, maxTokensKey.DefaultValue);

        var temperatureKey = config.Keys.First(k => k.Name == AnthropicConnectorConfig.TemperatureConfig);
        Assert.Equal(AnthropicConnectorConfig.DefaultTemperature, temperatureKey.DefaultValue);
    }

    [Fact]
    public void AnthropicConnectorConfig_HasValidModelConstants()
    {
        // Verify model constants are defined
        Assert.Equal("claude-3-5-sonnet-latest", AnthropicConnectorConfig.ModelClaude35Sonnet);
        Assert.Equal("claude-3-5-haiku-latest", AnthropicConnectorConfig.ModelClaude35Haiku);
        Assert.Equal("claude-3-opus-latest", AnthropicConnectorConfig.ModelClaude3Opus);
        Assert.Equal("claude-sonnet-4-20250514", AnthropicConnectorConfig.ModelClaudeSonnet4);
        Assert.Equal("claude-opus-4-20250514", AnthropicConnectorConfig.ModelClaudeOpus4);
    }

    private static ConnectorContext CreateContext()
    {
        return new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { },
            Logger = null
        };
    }
}
