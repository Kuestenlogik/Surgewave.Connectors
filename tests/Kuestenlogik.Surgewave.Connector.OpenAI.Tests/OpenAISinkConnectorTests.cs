namespace Kuestenlogik.Surgewave.Connector.OpenAI.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class OpenAISinkConnectorTests
{
    [Fact]
    public void OpenAISinkConnector_HasCorrectVersion()
    {
        using var connector = new OpenAISinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void OpenAISinkConnector_HasCorrectTaskClass()
    {
        using var connector = new OpenAISinkConnector();
        Assert.Equal(typeof(OpenAISinkTask), connector.TaskClass);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.ApiKeyConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasEmbeddingsKeys()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.EmbeddingsModelConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.EmbeddingsDimensionsConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.OutputFieldConfig);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasCompletionsKeys()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.CompletionsModelConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.SystemPromptConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.MaxTokensConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.TemperatureConfig);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void OpenAISinkConnector_Start_ThrowsOnMissingApiKey()
    {
        using var connector = new OpenAISinkConnector();
        connector.Initialize(CreateContext());

        // Clear environment variable if set
        var originalApiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("OPENAI_API_KEY", null);

            var config = new Dictionary<string, string>
            {
                [OpenAIConnectorConfig.TopicsConfig] = "test-topic"
            };

            var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
            Assert.Contains(OpenAIConnectorConfig.ApiKeyConfig, ex.Message);
        }
        finally
        {
            Environment.SetEnvironmentVariable("OPENAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void OpenAISinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new OpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(OpenAIConnectorConfig.TopicsConfig, ex.Message);
    }

    [Theory]
    [InlineData(OpenAIConnectorConfig.ModeEmbeddings)]
    [InlineData(OpenAIConnectorConfig.ModeCompletions)]
    [InlineData(OpenAIConnectorConfig.ModeSpeech)]
    [InlineData(OpenAIConnectorConfig.ModeTranscription)]
    [InlineData(OpenAIConnectorConfig.ModeImages)]
    [InlineData(OpenAIConnectorConfig.ModeModeration)]
    public void OpenAISinkConnector_Start_AcceptsValidModes(string mode)
    {
        using var connector = new OpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.ModeConfig] = mode
        };

        // Completions mode requires system prompt
        if (mode == OpenAIConnectorConfig.ModeCompletions)
        {
            config[OpenAIConnectorConfig.SystemPromptConfig] = "Test prompt";
        }

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void OpenAISinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new OpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void OpenAISinkConnector_Start_CompletionsModeRequiresSystemPrompt()
    {
        using var connector = new OpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic",
            [OpenAIConnectorConfig.ModeConfig] = OpenAIConnectorConfig.ModeCompletions
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(OpenAIConnectorConfig.SystemPromptConfig, ex.Message);
    }

    [Fact]
    public void OpenAISinkConnector_Start_AcceptsApiKeyFromEnvironment()
    {
        using var connector = new OpenAISinkConnector();
        connector.Initialize(CreateContext());

        var originalApiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");
        try
        {
            Environment.SetEnvironmentVariable("OPENAI_API_KEY", "test-env-api-key");

            var config = new Dictionary<string, string>
            {
                [OpenAIConnectorConfig.TopicsConfig] = "test-topic"
            };

            // Should not throw - API key from environment
            connector.Start(config);
            connector.Stop();
        }
        finally
        {
            Environment.SetEnvironmentVariable("OPENAI_API_KEY", originalApiKey);
        }
    }

    [Fact]
    public void OpenAISinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new OpenAISinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [OpenAIConnectorConfig.ApiKeyConfig] = "test-api-key",
            [OpenAIConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-api-key", taskConfigs[0][OpenAIConnectorConfig.ApiKeyConfig]);
        Assert.Equal("test-topic", taskConfigs[0][OpenAIConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.ModeConfig);
        Assert.Equal(OpenAIConnectorConfig.ModeEmbeddings, modeKey.DefaultValue);

        var embeddingsModelKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.EmbeddingsModelConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultEmbeddingsModel, embeddingsModelKey.DefaultValue);

        var completionsModelKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.CompletionsModelConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultCompletionsModel, completionsModelKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)OpenAIConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var maxTokensKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.MaxTokensConfig);
        Assert.Equal((long)OpenAIConnectorConfig.DefaultMaxTokens, maxTokensKey.DefaultValue);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasSpeechKeys()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.SpeechModelConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.SpeechVoiceConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.SpeechFormatConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.SpeechSpeedConfig);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasTranscriptionKeys()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.TranscriptionModelConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.TranscriptionLanguageConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.TranscriptionPromptConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.TranscriptionFormatConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.TranscriptionTimestampsConfig);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasImagesKeys()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.ImagesModelConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.ImagesSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.ImagesQualityConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.ImagesStyleConfig);
        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.ImagesCountConfig);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasModerationKeys()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == OpenAIConnectorConfig.ModerationModelConfig);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasSpeechDefaultValues()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        var speechModelKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.SpeechModelConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultSpeechModel, speechModelKey.DefaultValue);

        var speechVoiceKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.SpeechVoiceConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultSpeechVoice, speechVoiceKey.DefaultValue);

        var speechFormatKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.SpeechFormatConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultSpeechFormat, speechFormatKey.DefaultValue);

        var speechSpeedKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.SpeechSpeedConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultSpeechSpeed, speechSpeedKey.DefaultValue);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasTranscriptionDefaultValues()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        var transcriptionModelKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.TranscriptionModelConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultTranscriptionModel, transcriptionModelKey.DefaultValue);

        var transcriptionFormatKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.TranscriptionFormatConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultTranscriptionFormat, transcriptionFormatKey.DefaultValue);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasImagesDefaultValues()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        var imagesModelKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.ImagesModelConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultImagesModel, imagesModelKey.DefaultValue);

        var imagesSizeKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.ImagesSizeConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultImagesSize, imagesSizeKey.DefaultValue);

        var imagesQualityKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.ImagesQualityConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultImagesQuality, imagesQualityKey.DefaultValue);

        var imagesStyleKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.ImagesStyleConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultImagesStyle, imagesStyleKey.DefaultValue);

        var imagesCountKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.ImagesCountConfig);
        Assert.Equal((long)OpenAIConnectorConfig.DefaultImagesCount, imagesCountKey.DefaultValue);
    }

    [Fact]
    public void OpenAISinkConnector_Config_HasModerationDefaultValues()
    {
        using var connector = new OpenAISinkConnector();
        var config = connector.Config;

        var moderationModelKey = config.Keys.First(k => k.Name == OpenAIConnectorConfig.ModerationModelConfig);
        Assert.Equal(OpenAIConnectorConfig.DefaultModerationModel, moderationModelKey.DefaultValue);
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
