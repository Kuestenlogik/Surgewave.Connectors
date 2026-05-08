namespace Kuestenlogik.Surgewave.Connector.Gcp.Language.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class LanguageSinkTaskTests
{
    [Fact]
    public void LanguageSinkTask_HasCorrectVersion()
    {
        using var task = new LanguageSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void LanguageSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new LanguageSinkTask();
        task.Initialize(CreateTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void LanguageSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new LanguageSinkTask();
        task.Initialize(CreateTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task LanguageSinkTask_PutAsync_HandlesEmptyRecordsBeforeStart()
    {
        using var task = new LanguageSinkTask();
        task.Initialize(CreateTaskContext());

        // Should not throw on empty records even without starting
        await task.PutAsync(Array.Empty<SinkRecord>());
    }

    [Fact]
    public void LanguageConnectorConfig_HasCorrectTopicsConfig()
    {
        Assert.Equal("topics", LanguageConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void LanguageConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", LanguageConnectorConfig.ModeConfig);
        Assert.Equal("sentiment", LanguageConnectorConfig.ModeSentiment);
        Assert.Equal("entities", LanguageConnectorConfig.ModeEntities);
        Assert.Equal("syntax", LanguageConnectorConfig.ModeSyntax);
        Assert.Equal("classify", LanguageConnectorConfig.ModeClassify);
        Assert.Equal("all", LanguageConnectorConfig.ModeAll);
    }

    [Fact]
    public void LanguageConnectorConfig_HasCorrectCredentialsConfig()
    {
        Assert.Equal("gcp.project.id", LanguageConnectorConfig.ProjectIdConfig);
        Assert.Equal("gcp.credentials.json", LanguageConnectorConfig.CredentialsJsonConfig);
        Assert.Equal("gcp.credentials.path", LanguageConnectorConfig.CredentialsPathConfig);
    }

    [Fact]
    public void LanguageConnectorConfig_HasCorrectLanguageConfig()
    {
        Assert.Equal("language", LanguageConnectorConfig.LanguageConfig);
        Assert.Equal("en", LanguageConnectorConfig.DefaultLanguage);
    }

    [Fact]
    public void LanguageConnectorConfig_HasCorrectInputOutputConfig()
    {
        Assert.Equal("input.field", LanguageConnectorConfig.InputFieldConfig);
        Assert.Equal("text", LanguageConnectorConfig.DefaultInputField);
        Assert.Equal("output.field", LanguageConnectorConfig.OutputFieldConfig);
        Assert.Equal("analysis", LanguageConnectorConfig.DefaultOutputField);
    }

    [Fact]
    public void LanguageConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(10, LanguageConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, LanguageConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, LanguageConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, LanguageConnectorConfig.DefaultRetryBackoffMs);
    }

    [Fact]
    public void LanguageConnectorConfig_HasCorrectOutputConfig()
    {
        Assert.True(LanguageConnectorConfig.DefaultIncludeOriginal);
        Assert.Equal("json", LanguageConnectorConfig.FormatJson);
        Assert.Equal("merge", LanguageConnectorConfig.FormatMerge);
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
