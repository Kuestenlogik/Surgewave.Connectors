namespace Kuestenlogik.Surgewave.Connector.Azure.OpenAI.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class AzureOpenAISinkConnectorTests
{
    [Fact]
    public void AzureOpenAISinkConnector_HasCorrectVersion()
    {
        using var connector = new AzureOpenAISinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void AzureOpenAISinkConnector_HasCorrectTaskClass()
    {
        using var connector = new AzureOpenAISinkConnector();
        Assert.Equal(typeof(AzureOpenAISinkTask), connector.TaskClass);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new AzureOpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.EndpointConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.DeploymentIdConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Config_HasAuthenticationKeys()
    {
        using var connector = new AzureOpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.ApiKeyConfig && k.Type == ConfigType.Password);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new AzureOpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.OutputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.EmbeddingsFieldConfig);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Config_HasCompletionKeys()
    {
        using var connector = new AzureOpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.SystemPromptConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.MaxTokensConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.TemperatureConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.TopPConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.FrequencyPenaltyConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.PresencePenaltyConfig);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Config_HasDallEKeys()
    {
        using var connector = new AzureOpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.ImageSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.ImageQualityConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.ImageStyleConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.ImageCountConfig);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Config_HasWhisperKeys()
    {
        using var connector = new AzureOpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.AudioLanguageConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.WhisperModeConfig);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new AzureOpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == AzureOpenAIConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Start_ThrowsOnMissingEndpoint()
    {
        using var connector = new AzureOpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AzureOpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [AzureOpenAIConnectorConfig.DeploymentIdConfig] = "gpt-4"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(AzureOpenAIConnectorConfig.EndpointConfig, ex.Message);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Start_ThrowsOnMissingDeployment()
    {
        using var connector = new AzureOpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AzureOpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [AzureOpenAIConnectorConfig.EndpointConfig] = "https://test.openai.azure.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(AzureOpenAIConnectorConfig.DeploymentIdConfig, ex.Message);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new AzureOpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AzureOpenAIConnectorConfig.EndpointConfig] = "https://test.openai.azure.com",
            [AzureOpenAIConnectorConfig.DeploymentIdConfig] = "gpt-4"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(AzureOpenAIConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new AzureOpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AzureOpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [AzureOpenAIConnectorConfig.EndpointConfig] = "https://test.openai.azure.com",
            [AzureOpenAIConnectorConfig.DeploymentIdConfig] = "gpt-4",
            [AzureOpenAIConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Start_AcceptsValidChatConfig()
    {
        using var connector = new AzureOpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AzureOpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [AzureOpenAIConnectorConfig.EndpointConfig] = "https://test.openai.azure.com",
            [AzureOpenAIConnectorConfig.DeploymentIdConfig] = "gpt-4",
            [AzureOpenAIConnectorConfig.ModeConfig] = AzureOpenAIConnectorConfig.ModeChat
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void AzureOpenAISinkConnector_Start_AcceptsValidEmbeddingsConfig()
    {
        using var connector = new AzureOpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AzureOpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [AzureOpenAIConnectorConfig.EndpointConfig] = "https://test.openai.azure.com",
            [AzureOpenAIConnectorConfig.DeploymentIdConfig] = "text-embedding-ada-002",
            [AzureOpenAIConnectorConfig.ModeConfig] = AzureOpenAIConnectorConfig.ModeEmbeddings
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void AzureOpenAISinkConnector_Start_AcceptsValidDallEConfig()
    {
        using var connector = new AzureOpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AzureOpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [AzureOpenAIConnectorConfig.EndpointConfig] = "https://test.openai.azure.com",
            [AzureOpenAIConnectorConfig.DeploymentIdConfig] = "dall-e-3",
            [AzureOpenAIConnectorConfig.ModeConfig] = AzureOpenAIConnectorConfig.ModeDallE
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void AzureOpenAISinkConnector_Start_AcceptsValidWhisperConfig()
    {
        using var connector = new AzureOpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AzureOpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [AzureOpenAIConnectorConfig.EndpointConfig] = "https://test.openai.azure.com",
            [AzureOpenAIConnectorConfig.DeploymentIdConfig] = "whisper",
            [AzureOpenAIConnectorConfig.ModeConfig] = AzureOpenAIConnectorConfig.ModeWhisper
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void AzureOpenAISinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new AzureOpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [AzureOpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [AzureOpenAIConnectorConfig.EndpointConfig] = "https://test.openai.azure.com",
            [AzureOpenAIConnectorConfig.DeploymentIdConfig] = "gpt-4"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][AzureOpenAIConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void AzureOpenAISinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new AzureOpenAISinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == AzureOpenAIConnectorConfig.ModeConfig);
        Assert.Equal(AzureOpenAIConnectorConfig.ModeChat, modeKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == AzureOpenAIConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)AzureOpenAIConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var maxTokensKey = config.Keys.First(k => k.Name == AzureOpenAIConnectorConfig.MaxTokensConfig);
        Assert.Equal((long)AzureOpenAIConnectorConfig.DefaultMaxTokens, maxTokensKey.DefaultValue);

        var imageSizeKey = config.Keys.First(k => k.Name == AzureOpenAIConnectorConfig.ImageSizeConfig);
        Assert.Equal(AzureOpenAIConnectorConfig.DefaultImageSize, imageSizeKey.DefaultValue);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasValidModeConstants()
    {
        Assert.Equal("chat", AzureOpenAIConnectorConfig.ModeChat);
        Assert.Equal("embeddings", AzureOpenAIConnectorConfig.ModeEmbeddings);
        Assert.Equal("dalle", AzureOpenAIConnectorConfig.ModeDallE);
        Assert.Equal("whisper", AzureOpenAIConnectorConfig.ModeWhisper);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectDefaults()
    {
        Assert.Equal("text", AzureOpenAIConnectorConfig.DefaultInputField);
        Assert.Equal("response", AzureOpenAIConnectorConfig.DefaultOutputField);
        Assert.Equal("embedding", AzureOpenAIConnectorConfig.DefaultEmbeddingsField);
        Assert.Equal(10, AzureOpenAIConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, AzureOpenAIConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(4096, AzureOpenAIConnectorConfig.DefaultMaxTokens);
        Assert.Equal(0.7, AzureOpenAIConnectorConfig.DefaultTemperature);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectImageDefaults()
    {
        Assert.Equal("1024x1024", AzureOpenAIConnectorConfig.DefaultImageSize);
        Assert.Equal("standard", AzureOpenAIConnectorConfig.DefaultImageQuality);
        Assert.Equal("vivid", AzureOpenAIConnectorConfig.DefaultImageStyle);
        Assert.Equal(1, AzureOpenAIConnectorConfig.DefaultImageCount);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectWhisperDefaults()
    {
        Assert.Equal("transcribe", AzureOpenAIConnectorConfig.WhisperModeTranscribe);
        Assert.Equal("translate", AzureOpenAIConnectorConfig.WhisperModeTranslate);
        Assert.Equal("json", AzureOpenAIConnectorConfig.DefaultAudioFormat);
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
