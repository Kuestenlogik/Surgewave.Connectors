namespace Kuestenlogik.Surgewave.Connector.Grok.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class GrokSinkConnectorTests
{
    [Fact]
    public void GrokSinkConnector_HasCorrectVersion()
    {
        using var connector = new GrokSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void GrokSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new GrokSinkConnector();
        Assert.Equal(typeof(GrokSinkTask), connector.TaskClass);
    }

    [Fact]
    public void GrokSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new GrokSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.ApiKeyConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void GrokSinkConnector_Config_HasCompletionsKeys()
    {
        using var connector = new GrokSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.ModelConfig);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.SystemPromptConfig);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.MaxTokensConfig);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.TemperatureConfig);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.TopPConfig);
    }

    [Fact]
    public void GrokSinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new GrokSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.OutputFieldConfig);
    }

    [Fact]
    public void GrokSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new GrokSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == GrokConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void GrokSinkConnector_Start_ThrowsOnMissingApiKey()
    {
        using var connector = new GrokSinkConnector();
        connector.Initialize(CreateContext());

        // Clear environment variable if set
        var originalApiKey = Environment.GetEnvironmentVariable("XAI_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", null);

            var config = new Dictionary<string, string>
            {
                [GrokConnectorConfig.TopicsConfig] = "test-topic"
            };

            var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
            Assert.Contains(GrokConnectorConfig.ApiKeyConfig, ex.Message);
        }
        finally
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void GrokSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new GrokSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GrokConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void GrokSinkConnector_Start_ThrowsOnMissingSystemPrompt()
    {
        using var connector = new GrokSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.ModeConfig] = GrokConnectorConfig.ModeCompletions
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(GrokConnectorConfig.SystemPromptConfig, ex.Message);
    }

    [Fact]
    public void GrokSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new GrokSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void GrokSinkConnector_Start_AcceptsValidConfig()
    {
        using var connector = new GrokSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.ModeConfig] = GrokConnectorConfig.ModeCompletions,
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void GrokSinkConnector_Start_AcceptsApiKeyFromEnvironment()
    {
        using var connector = new GrokSinkConnector();
        connector.Initialize(CreateContext());

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
            connector.Start(config);
            connector.Stop();
        }
        finally
        {
            Environment.SetEnvironmentVariable("XAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void GrokSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new GrokSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [GrokConnectorConfig.ApiKeyConfig] = "test-api-key",
            [GrokConnectorConfig.TopicsConfig] = "test-topic",
            [GrokConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-api-key", taskConfigs[0][GrokConnectorConfig.ApiKeyConfig]);
        Assert.Equal("test-topic", taskConfigs[0][GrokConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void GrokSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new GrokSinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == GrokConnectorConfig.ModeConfig);
        Assert.Equal(GrokConnectorConfig.ModeCompletions, modeKey.DefaultValue);

        var modelKey = config.Keys.First(k => k.Name == GrokConnectorConfig.ModelConfig);
        Assert.Equal(GrokConnectorConfig.DefaultModel, modelKey.DefaultValue);

        var baseUrlKey = config.Keys.First(k => k.Name == GrokConnectorConfig.BaseUrlConfig);
        Assert.Equal(GrokConnectorConfig.DefaultBaseUrl, baseUrlKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == GrokConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)GrokConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var maxTokensKey = config.Keys.First(k => k.Name == GrokConnectorConfig.MaxTokensConfig);
        Assert.Equal((long)GrokConnectorConfig.DefaultMaxTokens, maxTokensKey.DefaultValue);

        var temperatureKey = config.Keys.First(k => k.Name == GrokConnectorConfig.TemperatureConfig);
        Assert.Equal(GrokConnectorConfig.DefaultTemperature, temperatureKey.DefaultValue);
    }

    [Fact]
    public void GrokConnectorConfig_HasValidModelConstants()
    {
        // Verify model constants are defined
        Assert.Equal("grok-3", GrokConnectorConfig.ModelGrok3);
        Assert.Equal("grok-3-mini", GrokConnectorConfig.ModelGrok3Mini);
        Assert.Equal("grok-2", GrokConnectorConfig.ModelGrok2);
        Assert.Equal("grok-2-mini", GrokConnectorConfig.ModelGrok2Mini);
    }

    [Fact]
    public void GrokConnectorConfig_HasCorrectDefaultBaseUrl()
    {
        Assert.Equal("https://api.x.ai/v1", GrokConnectorConfig.DefaultBaseUrl);
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
