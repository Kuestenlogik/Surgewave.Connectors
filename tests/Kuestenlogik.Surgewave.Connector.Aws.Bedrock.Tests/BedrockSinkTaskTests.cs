namespace Kuestenlogik.Surgewave.Connector.Aws.Bedrock.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class BedrockSinkTaskTests
{
    [Fact]
    public void BedrockSinkTask_HasCorrectVersion()
    {
        using var task = new BedrockSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void BedrockSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new BedrockSinkTask();
        task.Initialize(CreateTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void BedrockSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new BedrockSinkTask();
        task.Initialize(CreateTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task BedrockSinkTask_PutAsync_HandlesEmptyRecordsBeforeStart()
    {
        using var task = new BedrockSinkTask();
        task.Initialize(CreateTaskContext());

        // Should not throw on empty records even without starting
        await task.PutAsync(Array.Empty<SinkRecord>());
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectTopicsConfig()
    {
        Assert.Equal("topics", BedrockConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", BedrockConnectorConfig.ModeConfig);
        Assert.Equal("chat", BedrockConnectorConfig.ModeChat);
        Assert.Equal("embeddings", BedrockConnectorConfig.ModeEmbeddings);
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectAwsConfig()
    {
        Assert.Equal("aws.region", BedrockConnectorConfig.RegionConfig);
        Assert.Equal("aws.access.key.id", BedrockConnectorConfig.AccessKeyConfig);
        Assert.Equal("aws.secret.access.key", BedrockConnectorConfig.SecretKeyConfig);
        Assert.Equal("aws.endpoint", BedrockConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectModelConfig()
    {
        Assert.Equal("model.id", BedrockConnectorConfig.ModelIdConfig);
        Assert.Contains("anthropic.claude", BedrockConnectorConfig.DefaultModelId);
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectCompletionConfig()
    {
        Assert.Equal("system.prompt", BedrockConnectorConfig.SystemPromptConfig);
        Assert.Equal("max.tokens", BedrockConnectorConfig.MaxTokensConfig);
        Assert.Equal("temperature", BedrockConnectorConfig.TemperatureConfig);
        Assert.Equal("top.p", BedrockConnectorConfig.TopPConfig);
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectInputOutputConfig()
    {
        Assert.Equal("input.field", BedrockConnectorConfig.InputFieldConfig);
        Assert.Equal("text", BedrockConnectorConfig.DefaultInputField);
        Assert.Equal("output.field", BedrockConnectorConfig.OutputFieldConfig);
        Assert.Equal("response", BedrockConnectorConfig.DefaultOutputField);
        Assert.Equal("embeddings.field", BedrockConnectorConfig.EmbeddingsFieldConfig);
        Assert.Equal("embedding", BedrockConnectorConfig.DefaultEmbeddingsField);
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(10, BedrockConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, BedrockConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, BedrockConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, BedrockConnectorConfig.DefaultRetryBackoffMs);
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectCompletionDefaults()
    {
        Assert.Equal(4096, BedrockConnectorConfig.DefaultMaxTokens);
        Assert.Equal(0.7, BedrockConnectorConfig.DefaultTemperature);
        Assert.Equal(0.9, BedrockConnectorConfig.DefaultTopP);
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectOutputConfig()
    {
        Assert.True(BedrockConnectorConfig.DefaultIncludeOriginal);
        Assert.Equal("json", BedrockConnectorConfig.FormatJson);
        Assert.Equal("merge", BedrockConnectorConfig.FormatMerge);
    }

    [Fact]
    public void BedrockConnectorConfig_HasValidModelConstants()
    {
        Assert.Contains("anthropic.claude-3-5-sonnet", BedrockConnectorConfig.ModelClaudeSonnet35);
        Assert.Contains("anthropic.claude-3-5-haiku", BedrockConnectorConfig.ModelClaudeHaiku35);
        Assert.Contains("anthropic.claude-3-opus", BedrockConnectorConfig.ModelClaudeOpus);
        Assert.Contains("meta.llama3-70b", BedrockConnectorConfig.ModelLlama370B);
        Assert.Contains("meta.llama3-8b", BedrockConnectorConfig.ModelLlama38B);
        Assert.Contains("amazon.titan-text", BedrockConnectorConfig.ModelTitanText);
        Assert.Contains("amazon.titan-embed", BedrockConnectorConfig.ModelTitanEmbed);
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
