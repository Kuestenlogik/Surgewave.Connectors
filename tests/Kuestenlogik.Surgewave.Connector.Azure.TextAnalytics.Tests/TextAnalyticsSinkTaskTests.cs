namespace Kuestenlogik.Surgewave.Connector.Azure.TextAnalytics.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class TextAnalyticsSinkTaskTests
{
    [Fact]
    public void TextAnalyticsSinkTask_HasCorrectVersion()
    {
        using var task = new TextAnalyticsSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void TextAnalyticsSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new TextAnalyticsSinkTask();
        task.Initialize(CreateTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void TextAnalyticsSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new TextAnalyticsSinkTask();
        task.Initialize(CreateTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task TextAnalyticsSinkTask_PutAsync_HandlesEmptyRecordsBeforeStart()
    {
        using var task = new TextAnalyticsSinkTask();
        task.Initialize(CreateTaskContext());

        // Should not throw on empty records even without starting
        await task.PutAsync(Array.Empty<SinkRecord>(), CancellationToken.None);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasCorrectTopicsConfig()
    {
        Assert.Equal("topics", TextAnalyticsConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", TextAnalyticsConnectorConfig.ModeConfig);
        Assert.Equal("sentiment", TextAnalyticsConnectorConfig.ModeSentiment);
        Assert.Equal("entities", TextAnalyticsConnectorConfig.ModeEntities);
        Assert.Equal("key-phrases", TextAnalyticsConnectorConfig.ModeKeyPhrases);
        Assert.Equal("language-detection", TextAnalyticsConnectorConfig.ModeLanguageDetection);
        Assert.Equal("pii", TextAnalyticsConnectorConfig.ModePii);
        Assert.Equal("linked-entities", TextAnalyticsConnectorConfig.ModeLinkedEntities);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasCorrectConnectionConfig()
    {
        Assert.Equal("azure.text.analytics.endpoint", TextAnalyticsConnectorConfig.EndpointConfig);
        Assert.Equal("azure.text.analytics.api.key", TextAnalyticsConnectorConfig.ApiKeyConfig);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasCorrectInputOutputConfig()
    {
        Assert.Equal("input.field", TextAnalyticsConnectorConfig.InputFieldConfig);
        Assert.Equal("text", TextAnalyticsConnectorConfig.DefaultInputField);
        Assert.Equal("output.field", TextAnalyticsConnectorConfig.OutputFieldConfig);
        Assert.Equal("result", TextAnalyticsConnectorConfig.DefaultOutputField);
        Assert.Equal("language", TextAnalyticsConnectorConfig.LanguageConfig);
        Assert.Equal("en", TextAnalyticsConnectorConfig.DefaultLanguage);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasCorrectPiiConfig()
    {
        Assert.Equal("pii.categories", TextAnalyticsConnectorConfig.PiiCategoriesConfig);
        Assert.Equal("pii.domain", TextAnalyticsConnectorConfig.PiiDomainConfig);
        Assert.Equal("none", TextAnalyticsConnectorConfig.PiiDomainDefault);
        Assert.Equal("phi", TextAnalyticsConnectorConfig.PiiDomainHealthcare);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasCorrectSummarizationConfig()
    {
        Assert.Equal("max.sentence.count", TextAnalyticsConnectorConfig.MaxSentenceCountConfig);
        Assert.Equal(3, TextAnalyticsConnectorConfig.DefaultMaxSentenceCount);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(10, TextAnalyticsConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, TextAnalyticsConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, TextAnalyticsConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, TextAnalyticsConnectorConfig.DefaultRetryBackoffMs);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasCorrectOutputConfig()
    {
        Assert.True(TextAnalyticsConnectorConfig.DefaultIncludeOriginal);
        Assert.Equal("json", TextAnalyticsConnectorConfig.FormatJson);
        Assert.Equal("merge", TextAnalyticsConnectorConfig.FormatMerge);
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
