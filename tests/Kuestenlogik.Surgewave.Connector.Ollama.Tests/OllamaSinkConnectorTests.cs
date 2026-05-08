namespace Kuestenlogik.Surgewave.Connector.Ollama.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class OllamaSinkConnectorTests
{
    [Fact]
    public void OllamaSinkConnector_HasCorrectVersion()
    {
        using var connector = new OllamaSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void OllamaSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new OllamaSinkConnector();
        Assert.Equal(typeof(OllamaSinkTask), connector.TaskClass);
    }

    [Fact]
    public void OllamaSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new OllamaSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.BaseUrlConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.TopicsConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.ModeConfig);
    }

    [Fact]
    public void OllamaSinkConnector_Config_HasEmbeddingsKeys()
    {
        using var connector = new OllamaSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.EmbeddingsModelConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.OutputFieldConfig);
    }

    [Fact]
    public void OllamaSinkConnector_Config_HasCompletionsKeys()
    {
        using var connector = new OllamaSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.CompletionsModelConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.SystemPromptConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.MaxTokensConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.TemperatureConfig);
    }

    [Fact]
    public void OllamaSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new OllamaSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void OllamaSinkConnector_Config_HasOllamaSpecificKeys()
    {
        using var connector = new OllamaSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.KeepAliveConfig);
    }

    [Fact]
    public void OllamaSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new OllamaSinkConnector();
        var config = connector.Config;

        var baseUrlKey = config.Keys.First(k => k.Name == OllamaConnectorConfig.BaseUrlConfig);
        Assert.Equal(OllamaConnectorConfig.DefaultBaseUrl, baseUrlKey.DefaultValue);

        var modeKey = config.Keys.First(k => k.Name == OllamaConnectorConfig.ModeConfig);
        Assert.Equal(OllamaConnectorConfig.ModeEmbeddings, modeKey.DefaultValue);

        var embeddingsModelKey = config.Keys.First(k => k.Name == OllamaConnectorConfig.EmbeddingsModelConfig);
        Assert.Equal(OllamaConnectorConfig.DefaultEmbeddingsModel, embeddingsModelKey.DefaultValue);

        var completionsModelKey = config.Keys.First(k => k.Name == OllamaConnectorConfig.CompletionsModelConfig);
        Assert.Equal(OllamaConnectorConfig.DefaultCompletionsModel, completionsModelKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == OllamaConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)OllamaConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var keepAliveKey = config.Keys.First(k => k.Name == OllamaConnectorConfig.KeepAliveConfig);
        Assert.Equal(OllamaConnectorConfig.DefaultKeepAlive, keepAliveKey.DefaultValue);
    }

    [Fact]
    public void OllamaSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.BaseUrlConfig] = "http://localhost:11434"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void OllamaSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void OllamaSinkConnector_Start_AcceptsEmbeddingsMode()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeEmbeddings
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OllamaSinkConnector_Start_AcceptsCompletionsMode()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeCompletions
        };

        // Should not throw - system prompt is optional for Ollama
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OllamaSinkConnector_Start_AcceptsCustomBaseUrl()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.BaseUrlConfig] = "http://remote-ollama:11434"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OllamaSinkConnector_Start_AcceptsCustomModels()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeEmbeddings,
            [OllamaConnectorConfig.EmbeddingsModelConfig] = "mxbai-embed-large"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OllamaSinkConnector_Start_AcceptsCompletionsWithSystemPrompt()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.ModeConfig] = OllamaConnectorConfig.ModeCompletions,
            [OllamaConnectorConfig.CompletionsModelConfig] = "mistral",
            [OllamaConnectorConfig.SystemPromptConfig] = "Summarize the following text:"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OllamaSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.EmbeddingsModelConfig] = "nomic-embed-text"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][OllamaConnectorConfig.TopicsConfig]);
        Assert.Equal("nomic-embed-text", taskConfigs[0][OllamaConnectorConfig.EmbeddingsModelConfig]);
    }

    [Fact]
    public void OllamaSinkConnector_Stop_ClearsConfig()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        connector.Stop();

        // Multiple stops should be safe
        connector.Stop();
    }

    [Fact]
    public void OllamaSinkConnector_Config_HasWebhookKey()
    {
        using var connector = new OllamaSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OllamaConnectorConfig.WebhookUrlConfig);
    }

    [Fact]
    public void OllamaSinkConnector_Start_AcceptsWebhookUrl()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.WebhookUrlConfig] = "https://example.com/webhook"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OllamaSinkConnector_Start_AcceptsKeepAliveConfig()
    {
        using var connector = new OllamaSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OllamaConnectorConfig.TopicsConfig] = "test-topic",
            [OllamaConnectorConfig.KeepAliveConfig] = "10m"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    private static ConnectorContext CreateContext()
    {
        return new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
    }
}
