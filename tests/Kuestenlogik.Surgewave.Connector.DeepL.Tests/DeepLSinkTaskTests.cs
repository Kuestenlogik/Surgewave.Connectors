namespace Kuestenlogik.Surgewave.Connector.DeepL.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class DeepLSinkTaskTests
{
    [Fact]
    public void DeepLSinkTask_HasCorrectVersion()
    {
        using var task = new DeepLSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void DeepLSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new DeepLSinkTask();
        task.Initialize(CreateTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void DeepLSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new DeepLSinkTask();
        task.Initialize(CreateTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task DeepLSinkTask_PutAsync_HandlesEmptyRecordsBeforeStart()
    {
        using var task = new DeepLSinkTask();
        task.Initialize(CreateTaskContext());

        // Should not throw on empty records even without starting
        await task.PutAsync(Array.Empty<SinkRecord>(), CancellationToken.None);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectTopicsConfig()
    {
        Assert.Equal("topics", DeepLConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", DeepLConnectorConfig.ModeConfig);
        Assert.Equal("translate", DeepLConnectorConfig.ModeTranslate);
        Assert.Equal("detect-language", DeepLConnectorConfig.ModeDetectLanguage);
        Assert.Equal("usage", DeepLConnectorConfig.ModeUsage);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectApiConfig()
    {
        Assert.Equal("deepl.api.key", DeepLConnectorConfig.ApiKeyConfig);
        Assert.Equal("deepl.server.url", DeepLConnectorConfig.ServerUrlConfig);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectInputOutputConfig()
    {
        Assert.Equal("input.field", DeepLConnectorConfig.InputFieldConfig);
        Assert.Equal("text", DeepLConnectorConfig.DefaultInputField);
        Assert.Equal("output.field", DeepLConnectorConfig.OutputFieldConfig);
        Assert.Equal("result", DeepLConnectorConfig.DefaultOutputField);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectTranslationConfig()
    {
        Assert.Equal("source.language", DeepLConnectorConfig.SourceLanguageConfig);
        Assert.Equal("", DeepLConnectorConfig.DefaultSourceLanguage);
        Assert.Equal("target.language", DeepLConnectorConfig.TargetLanguageConfig);
        Assert.Equal("EN-US", DeepLConnectorConfig.DefaultTargetLanguage);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectFormalityConfig()
    {
        Assert.Equal("formality", DeepLConnectorConfig.FormalityConfig);
        Assert.Equal("default", DeepLConnectorConfig.FormalityDefault);
        Assert.Equal("more", DeepLConnectorConfig.FormalityMore);
        Assert.Equal("less", DeepLConnectorConfig.FormalityLess);
        Assert.Equal("prefer_more", DeepLConnectorConfig.FormalityPreferMore);
        Assert.Equal("prefer_less", DeepLConnectorConfig.FormalityPreferLess);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectAdvancedConfig()
    {
        Assert.Equal("context", DeepLConnectorConfig.ContextConfig);
        Assert.Equal("glossary.id", DeepLConnectorConfig.GlossaryIdConfig);
        Assert.Equal("tag.handling", DeepLConnectorConfig.TagHandlingConfig);
        Assert.Equal("xml", DeepLConnectorConfig.TagHandlingXml);
        Assert.Equal("html", DeepLConnectorConfig.TagHandlingHtml);
        Assert.Equal("preserve.formatting", DeepLConnectorConfig.PreserveFormattingConfig);
        Assert.False(DeepLConnectorConfig.DefaultPreserveFormatting);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectSplitSentencesConfig()
    {
        Assert.Equal("split.sentences", DeepLConnectorConfig.SplitSentencesConfig);
        Assert.Equal("none", DeepLConnectorConfig.SplitSentencesNone);
        Assert.Equal("all", DeepLConnectorConfig.SplitSentencesAll);
        Assert.Equal("punctuation", DeepLConnectorConfig.SplitSentencesPunctuation);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(25, DeepLConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, DeepLConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, DeepLConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, DeepLConnectorConfig.DefaultRetryBackoffMs);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectOutputConfig()
    {
        Assert.True(DeepLConnectorConfig.DefaultIncludeOriginal);
        Assert.True(DeepLConnectorConfig.DefaultIncludeDetectedLanguage);
        Assert.Equal("json", DeepLConnectorConfig.FormatJson);
        Assert.Equal("merge", DeepLConnectorConfig.FormatMerge);
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
