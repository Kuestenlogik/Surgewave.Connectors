namespace Kuestenlogik.Surgewave.Connector.HuggingFace.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class HuggingFaceSinkConnectorTests
{
    [Fact]
    public void HuggingFaceSinkConnector_HasCorrectVersion()
    {
        using var connector = new HuggingFaceSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void HuggingFaceSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new HuggingFaceSinkConnector();
        Assert.Equal(typeof(HuggingFaceSinkTask), connector.TaskClass);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new HuggingFaceSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.ModeConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.ModelIdConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Config_HasAuthenticationKeys()
    {
        using var connector = new HuggingFaceSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.ApiKeyConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new HuggingFaceSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.OutputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.EmbeddingsFieldConfig);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Config_HasTextGenerationKeys()
    {
        using var connector = new HuggingFaceSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.MaxNewTokensConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.TemperatureConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.TopKConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.TopPConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.DoSampleConfig);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Config_HasClassificationKeys()
    {
        using var connector = new HuggingFaceSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.CandidateLabelsConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.MultiLabelConfig);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Config_HasQuestionAnsweringKeys()
    {
        using var connector = new HuggingFaceSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.ContextFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.QuestionFieldConfig);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new HuggingFaceSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == HuggingFaceConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new HuggingFaceSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(HuggingFaceConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new HuggingFaceSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [HuggingFaceConnectorConfig.TopicsConfig] = "test-topic",
            [HuggingFaceConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Start_AcceptsValidSentimentConfig()
    {
        using var connector = new HuggingFaceSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [HuggingFaceConnectorConfig.TopicsConfig] = "test-topic",
            [HuggingFaceConnectorConfig.ModeConfig] = HuggingFaceConnectorConfig.ModeSentiment
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void HuggingFaceSinkConnector_Start_AcceptsValidNerConfig()
    {
        using var connector = new HuggingFaceSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [HuggingFaceConnectorConfig.TopicsConfig] = "test-topic",
            [HuggingFaceConnectorConfig.ModeConfig] = HuggingFaceConnectorConfig.ModeNer
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void HuggingFaceSinkConnector_Start_AcceptsValidEmbeddingsConfig()
    {
        using var connector = new HuggingFaceSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [HuggingFaceConnectorConfig.TopicsConfig] = "test-topic",
            [HuggingFaceConnectorConfig.ModeConfig] = HuggingFaceConnectorConfig.ModeEmbeddings
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void HuggingFaceSinkConnector_Start_AcceptsValidTextGenerationConfig()
    {
        using var connector = new HuggingFaceSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [HuggingFaceConnectorConfig.TopicsConfig] = "test-topic",
            [HuggingFaceConnectorConfig.ModeConfig] = HuggingFaceConnectorConfig.ModeTextGeneration
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void HuggingFaceSinkConnector_Start_AcceptsValidQuestionAnsweringConfig()
    {
        using var connector = new HuggingFaceSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [HuggingFaceConnectorConfig.TopicsConfig] = "test-topic",
            [HuggingFaceConnectorConfig.ModeConfig] = HuggingFaceConnectorConfig.ModeQuestionAnswering
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void HuggingFaceSinkConnector_Start_AcceptsValidClassificationConfig()
    {
        using var connector = new HuggingFaceSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [HuggingFaceConnectorConfig.TopicsConfig] = "test-topic",
            [HuggingFaceConnectorConfig.ModeConfig] = HuggingFaceConnectorConfig.ModeClassification
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void HuggingFaceSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new HuggingFaceSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [HuggingFaceConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][HuggingFaceConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void HuggingFaceSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new HuggingFaceSinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == HuggingFaceConnectorConfig.ModeConfig);
        Assert.Equal(HuggingFaceConnectorConfig.ModeSentiment, modeKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == HuggingFaceConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)HuggingFaceConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var endpointKey = config.Keys.First(k => k.Name == HuggingFaceConnectorConfig.EndpointConfig);
        Assert.Equal(HuggingFaceConnectorConfig.DefaultEndpoint, endpointKey.DefaultValue);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasValidModeConstants()
    {
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
    public void HuggingFaceConnectorConfig_HasCorrectDefaults()
    {
        Assert.Equal("text", HuggingFaceConnectorConfig.DefaultInputField);
        Assert.Equal("result", HuggingFaceConnectorConfig.DefaultOutputField);
        Assert.Equal("embedding", HuggingFaceConnectorConfig.DefaultEmbeddingsField);
        Assert.Equal(10, HuggingFaceConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, HuggingFaceConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(50, HuggingFaceConnectorConfig.DefaultMaxNewTokens);
    }

    [Fact]
    public void HuggingFaceConnectorConfig_HasCorrectDefaultModels()
    {
        Assert.NotEmpty(HuggingFaceConnectorConfig.DefaultSentimentModel);
        Assert.NotEmpty(HuggingFaceConnectorConfig.DefaultNerModel);
        Assert.NotEmpty(HuggingFaceConnectorConfig.DefaultClassificationModel);
        Assert.NotEmpty(HuggingFaceConnectorConfig.DefaultEmbeddingsModel);
        Assert.NotEmpty(HuggingFaceConnectorConfig.DefaultTextGenerationModel);
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
