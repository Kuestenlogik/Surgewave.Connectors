namespace Kuestenlogik.Surgewave.Connector.Gcp.VertexAI.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class VertexAISinkConnectorTests
{
    [Fact]
    public void VertexAISinkConnector_HasCorrectVersion()
    {
        using var connector = new VertexAISinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void VertexAISinkConnector_HasCorrectTaskClass()
    {
        using var connector = new VertexAISinkConnector();
        Assert.Equal(typeof(VertexAISinkTask), connector.TaskClass);
    }

    [Fact]
    public void VertexAISinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new VertexAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.ProjectIdConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void VertexAISinkConnector_Config_HasConnectionKeys()
    {
        using var connector = new VertexAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.LocationConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.CredentialsJsonConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.CredentialsPathConfig);
    }

    [Fact]
    public void VertexAISinkConnector_Config_HasCompletionsKeys()
    {
        using var connector = new VertexAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.ModelConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.SystemPromptConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.MaxTokensConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.TemperatureConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.TopPConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.TopKConfig);
    }

    [Fact]
    public void VertexAISinkConnector_Config_HasEmbeddingsKeys()
    {
        using var connector = new VertexAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.EmbeddingsModelConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.EmbeddingsDimensionsConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.EmbeddingsFieldConfig);
    }

    [Fact]
    public void VertexAISinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new VertexAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.OutputFieldConfig);
    }

    [Fact]
    public void VertexAISinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new VertexAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == VertexAIConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void VertexAISinkConnector_Start_ThrowsOnMissingProjectId()
    {
        using var connector = new VertexAISinkConnector();
        connector.Initialize(CreateContext());

        // Clear environment variable if set
        var originalProjectId = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT");
        try
        {
            Environment.SetEnvironmentVariable("GOOGLE_CLOUD_PROJECT", null);

            var config = new Dictionary<string, string>
            {
                [VertexAIConnectorConfig.TopicsConfig] = "test-topic"
            };

            var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
            Assert.Contains(VertexAIConnectorConfig.ProjectIdConfig, ex.Message);
        }
        finally
        {
            Environment.SetEnvironmentVariable("GOOGLE_CLOUD_PROJECT", originalProjectId);
        }
    }

    [Fact]
    public void VertexAISinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new VertexAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [VertexAIConnectorConfig.ProjectIdConfig] = "test-project"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(VertexAIConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void VertexAISinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new VertexAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [VertexAIConnectorConfig.ProjectIdConfig] = "test-project",
            [VertexAIConnectorConfig.TopicsConfig] = "test-topic",
            [VertexAIConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void VertexAISinkConnector_Start_ThrowsOnMissingSystemPrompt()
    {
        using var connector = new VertexAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [VertexAIConnectorConfig.ProjectIdConfig] = "test-project",
            [VertexAIConnectorConfig.TopicsConfig] = "test-topic",
            [VertexAIConnectorConfig.ModeConfig] = VertexAIConnectorConfig.ModeCompletions
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(VertexAIConnectorConfig.SystemPromptConfig, ex.Message);
    }

    [Fact]
    public void VertexAISinkConnector_Start_AcceptsValidCompletionsConfig()
    {
        using var connector = new VertexAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [VertexAIConnectorConfig.ProjectIdConfig] = "test-project",
            [VertexAIConnectorConfig.TopicsConfig] = "test-topic",
            [VertexAIConnectorConfig.ModeConfig] = VertexAIConnectorConfig.ModeCompletions,
            [VertexAIConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void VertexAISinkConnector_Start_AcceptsValidEmbeddingsConfig()
    {
        using var connector = new VertexAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [VertexAIConnectorConfig.ProjectIdConfig] = "test-project",
            [VertexAIConnectorConfig.TopicsConfig] = "test-topic",
            [VertexAIConnectorConfig.ModeConfig] = VertexAIConnectorConfig.ModeEmbeddings
        };

        // Should not throw - embeddings mode doesn't require system prompt
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void VertexAISinkConnector_Start_AcceptsProjectIdFromEnvironment()
    {
        using var connector = new VertexAISinkConnector();
        connector.Initialize(CreateContext());

        var originalProjectId = Environment.GetEnvironmentVariable("GOOGLE_CLOUD_PROJECT");
        try
        {
            Environment.SetEnvironmentVariable("GOOGLE_CLOUD_PROJECT", "test-env-project");

            var config = new Dictionary<string, string>
            {
                [VertexAIConnectorConfig.TopicsConfig] = "test-topic",
                [VertexAIConnectorConfig.SystemPromptConfig] = "Test system prompt"
            };

            // Should not throw - project ID from environment
            connector.Start(config);
            connector.Stop();
        }
        finally
        {
            Environment.SetEnvironmentVariable("GOOGLE_CLOUD_PROJECT", originalProjectId);
        }
    }

    [Fact]
    public void VertexAISinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new VertexAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [VertexAIConnectorConfig.ProjectIdConfig] = "test-project",
            [VertexAIConnectorConfig.TopicsConfig] = "test-topic",
            [VertexAIConnectorConfig.SystemPromptConfig] = "Test system prompt"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-project", taskConfigs[0][VertexAIConnectorConfig.ProjectIdConfig]);
        Assert.Equal("test-topic", taskConfigs[0][VertexAIConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void VertexAISinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new VertexAISinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == VertexAIConnectorConfig.ModeConfig);
        Assert.Equal(VertexAIConnectorConfig.ModeCompletions, modeKey.DefaultValue);

        var modelKey = config.Keys.First(k => k.Name == VertexAIConnectorConfig.ModelConfig);
        Assert.Equal(VertexAIConnectorConfig.DefaultModel, modelKey.DefaultValue);

        var locationKey = config.Keys.First(k => k.Name == VertexAIConnectorConfig.LocationConfig);
        Assert.Equal(VertexAIConnectorConfig.DefaultLocation, locationKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == VertexAIConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)VertexAIConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var maxTokensKey = config.Keys.First(k => k.Name == VertexAIConnectorConfig.MaxTokensConfig);
        Assert.Equal((long)VertexAIConnectorConfig.DefaultMaxTokens, maxTokensKey.DefaultValue);

        var temperatureKey = config.Keys.First(k => k.Name == VertexAIConnectorConfig.TemperatureConfig);
        Assert.Equal(VertexAIConnectorConfig.DefaultTemperature, temperatureKey.DefaultValue);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasValidModelConstants()
    {
        // Verify model constants are defined
        Assert.Equal("gemini-2.0-flash", VertexAIConnectorConfig.ModelGemini20Flash);
        Assert.Equal("gemini-2.0-flash-lite", VertexAIConnectorConfig.ModelGemini20FlashLite);
        Assert.Equal("gemini-1.5-pro", VertexAIConnectorConfig.ModelGemini15Pro);
        Assert.Equal("gemini-1.5-flash", VertexAIConnectorConfig.ModelGemini15Flash);
        Assert.Equal("gemini-pro", VertexAIConnectorConfig.ModelGeminiPro);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectDefaultLocation()
    {
        Assert.Equal("us-central1", VertexAIConnectorConfig.DefaultLocation);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectModeConstants()
    {
        Assert.Equal("completions", VertexAIConnectorConfig.ModeCompletions);
        Assert.Equal("embeddings", VertexAIConnectorConfig.ModeEmbeddings);
    }

    [Fact]
    public void VertexAIConnectorConfig_HasCorrectEmbeddingsDefaults()
    {
        Assert.Equal("text-embedding-005", VertexAIConnectorConfig.DefaultEmbeddingsModel);
        Assert.Equal(768, VertexAIConnectorConfig.DefaultEmbeddingsDimensions);
        Assert.Equal("embedding", VertexAIConnectorConfig.DefaultEmbeddingsField);
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
