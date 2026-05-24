namespace Kuestenlogik.Surgewave.Connector.Aws.Comprehend.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class ComprehendSinkTaskTests
{
    [Fact]
    public void ComprehendSinkTask_HasCorrectVersion()
    {
        using var task = new ComprehendSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void ComprehendSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new ComprehendSinkTask();
        task.Initialize(CreateTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void ComprehendSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new ComprehendSinkTask();
        task.Initialize(CreateTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task ComprehendSinkTask_PutAsync_HandlesEmptyRecordsBeforeStart()
    {
        using var task = new ComprehendSinkTask();
        task.Initialize(CreateTaskContext());

        // Should not throw on empty records even without starting
        await task.PutAsync(Array.Empty<SinkRecord>());
    }

    [Fact]
    public void ComprehendConnectorConfig_HasCorrectTopicsConfig()
    {
        Assert.Equal("topics", ComprehendConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void ComprehendConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", ComprehendConnectorConfig.ModeConfig);
        Assert.Equal("sentiment", ComprehendConnectorConfig.ModeSentiment);
        Assert.Equal("entities", ComprehendConnectorConfig.ModeEntities);
        Assert.Equal("key_phrases", ComprehendConnectorConfig.ModeKeyPhrases);
        Assert.Equal("language", ComprehendConnectorConfig.ModeLanguage);
        Assert.Equal("pii", ComprehendConnectorConfig.ModePii);
        Assert.Equal("syntax", ComprehendConnectorConfig.ModeSyntax);
        Assert.Equal("all", ComprehendConnectorConfig.ModeAll);
    }

    [Fact]
    public void ComprehendConnectorConfig_HasCorrectAwsConfig()
    {
        Assert.Equal("aws.region", ComprehendConnectorConfig.RegionConfig);
        Assert.Equal("aws.access.key.id", ComprehendConnectorConfig.AccessKeyConfig);
        Assert.Equal("aws.secret.access.key", ComprehendConnectorConfig.SecretKeyConfig);
        Assert.Equal("aws.endpoint", ComprehendConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void ComprehendConnectorConfig_HasCorrectLanguageConfig()
    {
        Assert.Equal("language", ComprehendConnectorConfig.LanguageConfig);
        Assert.Equal("en", ComprehendConnectorConfig.DefaultLanguage);
    }

    [Fact]
    public void ComprehendConnectorConfig_HasCorrectInputOutputConfig()
    {
        Assert.Equal("input.field", ComprehendConnectorConfig.InputFieldConfig);
        Assert.Equal("text", ComprehendConnectorConfig.DefaultInputField);
        Assert.Equal("output.field", ComprehendConnectorConfig.OutputFieldConfig);
        Assert.Equal("analysis", ComprehendConnectorConfig.DefaultOutputField);
    }

    [Fact]
    public void ComprehendConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(25, ComprehendConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, ComprehendConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, ComprehendConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, ComprehendConnectorConfig.DefaultRetryBackoffMs);
    }

    [Fact]
    public void ComprehendConnectorConfig_HasCorrectOutputConfig()
    {
        Assert.True(ComprehendConnectorConfig.DefaultIncludeOriginal);
        Assert.Equal("json", ComprehendConnectorConfig.FormatJson);
        Assert.Equal("merge", ComprehendConnectorConfig.FormatMerge);
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
