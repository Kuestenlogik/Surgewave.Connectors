namespace Kuestenlogik.Surgewave.Connector.Azure.OpenAI.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class AzureOpenAISinkTaskTests
{
    [Fact]
    public void AzureOpenAISinkTask_HasCorrectVersion()
    {
        using var task = new AzureOpenAISinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void AzureOpenAISinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new AzureOpenAISinkTask();
        task.Initialize(CreateTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void AzureOpenAISinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new AzureOpenAISinkTask();
        task.Initialize(CreateTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task AzureOpenAISinkTask_PutAsync_HandlesEmptyRecordsBeforeStart()
    {
        using var task = new AzureOpenAISinkTask();
        task.Initialize(CreateTaskContext());

        // Should not throw on empty records even without starting
        await task.PutAsync(Array.Empty<SinkRecord>());
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectTopicsConfig()
    {
        Assert.Equal("topics", AzureOpenAIConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", AzureOpenAIConnectorConfig.ModeConfig);
        Assert.Equal("chat", AzureOpenAIConnectorConfig.ModeChat);
        Assert.Equal("embeddings", AzureOpenAIConnectorConfig.ModeEmbeddings);
        Assert.Equal("dalle", AzureOpenAIConnectorConfig.ModeDallE);
        Assert.Equal("whisper", AzureOpenAIConnectorConfig.ModeWhisper);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectAzureConfig()
    {
        Assert.Equal("azure.openai.endpoint", AzureOpenAIConnectorConfig.EndpointConfig);
        Assert.Equal("azure.openai.api.key", AzureOpenAIConnectorConfig.ApiKeyConfig);
        Assert.Equal("azure.openai.deployment.id", AzureOpenAIConnectorConfig.DeploymentIdConfig);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectCompletionConfig()
    {
        Assert.Equal("system.prompt", AzureOpenAIConnectorConfig.SystemPromptConfig);
        Assert.Equal("max.tokens", AzureOpenAIConnectorConfig.MaxTokensConfig);
        Assert.Equal("temperature", AzureOpenAIConnectorConfig.TemperatureConfig);
        Assert.Equal("top.p", AzureOpenAIConnectorConfig.TopPConfig);
        Assert.Equal("frequency.penalty", AzureOpenAIConnectorConfig.FrequencyPenaltyConfig);
        Assert.Equal("presence.penalty", AzureOpenAIConnectorConfig.PresencePenaltyConfig);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectInputOutputConfig()
    {
        Assert.Equal("input.field", AzureOpenAIConnectorConfig.InputFieldConfig);
        Assert.Equal("text", AzureOpenAIConnectorConfig.DefaultInputField);
        Assert.Equal("output.field", AzureOpenAIConnectorConfig.OutputFieldConfig);
        Assert.Equal("response", AzureOpenAIConnectorConfig.DefaultOutputField);
        Assert.Equal("embeddings.field", AzureOpenAIConnectorConfig.EmbeddingsFieldConfig);
        Assert.Equal("embedding", AzureOpenAIConnectorConfig.DefaultEmbeddingsField);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(10, AzureOpenAIConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, AzureOpenAIConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, AzureOpenAIConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, AzureOpenAIConnectorConfig.DefaultRetryBackoffMs);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectCompletionDefaults()
    {
        Assert.Equal(4096, AzureOpenAIConnectorConfig.DefaultMaxTokens);
        Assert.Equal(0.7, AzureOpenAIConnectorConfig.DefaultTemperature);
        Assert.Equal(1.0, AzureOpenAIConnectorConfig.DefaultTopP);
        Assert.Equal(0.0, AzureOpenAIConnectorConfig.DefaultFrequencyPenalty);
        Assert.Equal(0.0, AzureOpenAIConnectorConfig.DefaultPresencePenalty);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectOutputConfig()
    {
        Assert.True(AzureOpenAIConnectorConfig.DefaultIncludeOriginal);
        Assert.Equal("json", AzureOpenAIConnectorConfig.FormatJson);
        Assert.Equal("merge", AzureOpenAIConnectorConfig.FormatMerge);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectDallEConfig()
    {
        Assert.Equal("image.size", AzureOpenAIConnectorConfig.ImageSizeConfig);
        Assert.Equal("1024x1024", AzureOpenAIConnectorConfig.DefaultImageSize);
        Assert.Equal("image.quality", AzureOpenAIConnectorConfig.ImageQualityConfig);
        Assert.Equal("standard", AzureOpenAIConnectorConfig.DefaultImageQuality);
        Assert.Equal("image.style", AzureOpenAIConnectorConfig.ImageStyleConfig);
        Assert.Equal("vivid", AzureOpenAIConnectorConfig.DefaultImageStyle);
        Assert.Equal("image.count", AzureOpenAIConnectorConfig.ImageCountConfig);
        Assert.Equal(1, AzureOpenAIConnectorConfig.DefaultImageCount);
    }

    [Fact]
    public void AzureOpenAIConnectorConfig_HasCorrectWhisperConfig()
    {
        Assert.Equal("audio.language", AzureOpenAIConnectorConfig.AudioLanguageConfig);
        Assert.Equal("audio.format", AzureOpenAIConnectorConfig.AudioFormatConfig);
        Assert.Equal("json", AzureOpenAIConnectorConfig.DefaultAudioFormat);
        Assert.Equal("whisper.mode", AzureOpenAIConnectorConfig.WhisperModeConfig);
        Assert.Equal("transcribe", AzureOpenAIConnectorConfig.DefaultWhisperMode);
        Assert.Equal("transcribe", AzureOpenAIConnectorConfig.WhisperModeTranscribe);
        Assert.Equal("translate", AzureOpenAIConnectorConfig.WhisperModeTranslate);
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
