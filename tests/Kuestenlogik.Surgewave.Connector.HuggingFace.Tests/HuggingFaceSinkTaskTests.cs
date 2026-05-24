namespace Kuestenlogik.Surgewave.Connector.HuggingFace.Tests;

using Kuestenlogik.Surgewave.Connect;
using Xunit;

public sealed class HuggingFaceSinkTaskTests
{
    [Fact]
    public void HuggingFaceSinkTask_HasCorrectVersion()
    {
        using var task = new HuggingFaceSinkTask();
        Assert.Equal("1.0.0", task.Version);
    }

    [Fact]
    public void HuggingFaceSinkTask_Stop_CanBeCalledMultipleTimes()
    {
        using var task = new HuggingFaceSinkTask();
        task.Initialize(CreateTaskContext());

        task.Stop();
        task.Stop(); // Should not throw
    }

    [Fact]
    public void HuggingFaceSinkTask_Dispose_CanBeCalledMultipleTimes()
    {
        var task = new HuggingFaceSinkTask();
        task.Initialize(CreateTaskContext());

        task.Dispose();
        task.Dispose(); // Should not throw
    }

    [Fact]
    public async Task HuggingFaceSinkTask_PutAsync_HandlesEmptyRecordsBeforeStart()
    {
        using var task = new HuggingFaceSinkTask();
        task.Initialize(CreateTaskContext());

        // Should not throw on empty records even without starting
        await task.PutAsync(Array.Empty<SinkRecord>());
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectTopicsConfig()
    {
        Assert.Equal("topics", HuggingFaceConnectorConfig.TopicsConfig);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectModeConfig()
    {
        Assert.Equal("mode", HuggingFaceConnectorConfig.ModeConfig);
        Assert.Equal("sentiment", HuggingFaceConnectorConfig.ModeSentiment);
        Assert.Equal("ner", HuggingFaceConnectorConfig.ModeNer);
        Assert.Equal("classification", HuggingFaceConnectorConfig.ModeClassification);
        Assert.Equal("embeddings", HuggingFaceConnectorConfig.ModeEmbeddings);
        Assert.Equal("text-generation", HuggingFaceConnectorConfig.ModeTextGeneration);
        Assert.Equal("fill-mask", HuggingFaceConnectorConfig.ModeFillMask);
        Assert.Equal("question-answering", HuggingFaceConnectorConfig.ModeQuestionAnswering);
        Assert.Equal("summarization", HuggingFaceConnectorConfig.ModeSummarization);
        Assert.Equal("translation", HuggingFaceConnectorConfig.ModeTranslation);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectConnectionConfig()
    {
        Assert.Equal("huggingface.api.key", HuggingFaceConnectorConfig.ApiKeyConfig);
        Assert.Equal("huggingface.model.id", HuggingFaceConnectorConfig.ModelIdConfig);
        Assert.Equal("huggingface.endpoint", HuggingFaceConnectorConfig.EndpointConfig);
        Assert.Equal("https://api-inference.huggingface.co/models", HuggingFaceConnectorConfig.DefaultEndpoint);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectInputOutputConfig()
    {
        Assert.Equal("input.field", HuggingFaceConnectorConfig.InputFieldConfig);
        Assert.Equal("text", HuggingFaceConnectorConfig.DefaultInputField);
        Assert.Equal("output.field", HuggingFaceConnectorConfig.OutputFieldConfig);
        Assert.Equal("result", HuggingFaceConnectorConfig.DefaultOutputField);
        Assert.Equal("embeddings.field", HuggingFaceConnectorConfig.EmbeddingsFieldConfig);
        Assert.Equal("embedding", HuggingFaceConnectorConfig.DefaultEmbeddingsField);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectQuestionAnsweringConfig()
    {
        Assert.Equal("context.field", HuggingFaceConnectorConfig.ContextFieldConfig);
        Assert.Equal("context", HuggingFaceConnectorConfig.DefaultContextField);
        Assert.Equal("question.field", HuggingFaceConnectorConfig.QuestionFieldConfig);
        Assert.Equal("question", HuggingFaceConnectorConfig.DefaultQuestionField);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectClassificationConfig()
    {
        Assert.Equal("candidate.labels", HuggingFaceConnectorConfig.CandidateLabelsConfig);
        Assert.Equal("multi.label", HuggingFaceConnectorConfig.MultiLabelConfig);
        Assert.False(HuggingFaceConnectorConfig.DefaultMultiLabel);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectTextGenerationConfig()
    {
        Assert.Equal("max.new.tokens", HuggingFaceConnectorConfig.MaxNewTokensConfig);
        Assert.Equal(50, HuggingFaceConnectorConfig.DefaultMaxNewTokens);
        Assert.Equal("temperature", HuggingFaceConnectorConfig.TemperatureConfig);
        Assert.Equal(1.0, HuggingFaceConnectorConfig.DefaultTemperature);
        Assert.Equal("top.k", HuggingFaceConnectorConfig.TopKConfig);
        Assert.Equal(50, HuggingFaceConnectorConfig.DefaultTopK);
        Assert.Equal("top.p", HuggingFaceConnectorConfig.TopPConfig);
        Assert.Equal(0.95, HuggingFaceConnectorConfig.DefaultTopP);
        Assert.Equal("do.sample", HuggingFaceConnectorConfig.DoSampleConfig);
        Assert.True(HuggingFaceConnectorConfig.DefaultDoSample);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectBatchingDefaults()
    {
        Assert.Equal(10, HuggingFaceConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, HuggingFaceConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, HuggingFaceConnectorConfig.DefaultRetryMax);
        Assert.Equal(1000, HuggingFaceConnectorConfig.DefaultRetryBackoffMs);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectOutputConfig()
    {
        Assert.True(HuggingFaceConnectorConfig.DefaultIncludeOriginal);
        Assert.Equal("json", HuggingFaceConnectorConfig.FormatJson);
        Assert.Equal("merge", HuggingFaceConnectorConfig.FormatMerge);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectDefaultModels()
    {
        Assert.Contains("distilbert", HuggingFaceConnectorConfig.DefaultSentimentModel);
        Assert.Contains("bert", HuggingFaceConnectorConfig.DefaultNerModel);
        Assert.Contains("bart", HuggingFaceConnectorConfig.DefaultClassificationModel);
        Assert.Contains("sentence-transformers", HuggingFaceConnectorConfig.DefaultEmbeddingsModel);
        Assert.Contains("gpt2", HuggingFaceConnectorConfig.DefaultTextGenerationModel);
        Assert.Contains("bert", HuggingFaceConnectorConfig.DefaultFillMaskModel);
        Assert.Contains("roberta", HuggingFaceConnectorConfig.DefaultQuestionAnsweringModel);
        Assert.Contains("bart", HuggingFaceConnectorConfig.DefaultSummarizationModel);
        Assert.Contains("Helsinki-NLP", HuggingFaceConnectorConfig.DefaultTranslationModel);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectWaitForModelConfig()
    {
        Assert.Equal("wait.for.model", HuggingFaceConnectorConfig.WaitForModelConfig);
        Assert.True(HuggingFaceConnectorConfig.DefaultWaitForModel);
    }

    private static TaskContext CreateTaskContext()
    {
        return new TaskContext
        {
            RaiseError = _ => { }
        };
    }
}
