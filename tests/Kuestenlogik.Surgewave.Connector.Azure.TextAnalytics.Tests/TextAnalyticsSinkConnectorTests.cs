namespace Kuestenlogik.Surgewave.Connector.Azure.TextAnalytics.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class TextAnalyticsSinkConnectorTests
{
    [Fact]
    public void TextAnalyticsSinkConnector_HasCorrectVersion()
    {
        using var connector = new TextAnalyticsSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new TextAnalyticsSinkConnector();
        Assert.Equal(typeof(TextAnalyticsSinkTask), connector.TaskClass);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new TextAnalyticsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.EndpointConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Config_HasAuthenticationKeys()
    {
        using var connector = new TextAnalyticsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.ApiKeyConfig && k.Type == ConfigType.Password);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new TextAnalyticsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.OutputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.LanguageConfig);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Config_HasPiiKeys()
    {
        using var connector = new TextAnalyticsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.PiiCategoriesConfig);
        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.PiiDomainConfig);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Config_HasSummarizationKeys()
    {
        using var connector = new TextAnalyticsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.MaxSentenceCountConfig);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new TextAnalyticsSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == TextAnalyticsConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Start_ThrowsOnMissingEndpoint()
    {
        using var connector = new TextAnalyticsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [TextAnalyticsConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TextAnalyticsConnectorConfig.EndpointConfig, ex.Message);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new TextAnalyticsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [TextAnalyticsConnectorConfig.EndpointConfig] = "https://test.cognitiveservices.azure.com"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(TextAnalyticsConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new TextAnalyticsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [TextAnalyticsConnectorConfig.TopicsConfig] = "test-topic",
            [TextAnalyticsConnectorConfig.EndpointConfig] = "https://test.cognitiveservices.azure.com",
            [TextAnalyticsConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Start_AcceptsValidSentimentConfig()
    {
        using var connector = new TextAnalyticsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [TextAnalyticsConnectorConfig.TopicsConfig] = "test-topic",
            [TextAnalyticsConnectorConfig.EndpointConfig] = "https://test.cognitiveservices.azure.com",
            [TextAnalyticsConnectorConfig.ModeConfig] = TextAnalyticsConnectorConfig.ModeSentiment
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Start_AcceptsValidEntitiesConfig()
    {
        using var connector = new TextAnalyticsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [TextAnalyticsConnectorConfig.TopicsConfig] = "test-topic",
            [TextAnalyticsConnectorConfig.EndpointConfig] = "https://test.cognitiveservices.azure.com",
            [TextAnalyticsConnectorConfig.ModeConfig] = TextAnalyticsConnectorConfig.ModeEntities
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Start_AcceptsValidKeyPhrasesConfig()
    {
        using var connector = new TextAnalyticsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [TextAnalyticsConnectorConfig.TopicsConfig] = "test-topic",
            [TextAnalyticsConnectorConfig.EndpointConfig] = "https://test.cognitiveservices.azure.com",
            [TextAnalyticsConnectorConfig.ModeConfig] = TextAnalyticsConnectorConfig.ModeKeyPhrases
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Start_AcceptsValidPiiConfig()
    {
        using var connector = new TextAnalyticsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [TextAnalyticsConnectorConfig.TopicsConfig] = "test-topic",
            [TextAnalyticsConnectorConfig.EndpointConfig] = "https://test.cognitiveservices.azure.com",
            [TextAnalyticsConnectorConfig.ModeConfig] = TextAnalyticsConnectorConfig.ModePii
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Start_AcceptsValidSummarizationConfig()
    {
        using var connector = new TextAnalyticsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [TextAnalyticsConnectorConfig.TopicsConfig] = "test-topic",
            [TextAnalyticsConnectorConfig.EndpointConfig] = "https://test.cognitiveservices.azure.com",
            [TextAnalyticsConnectorConfig.ModeConfig] = TextAnalyticsConnectorConfig.ModeSummarization
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void TextAnalyticsSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new TextAnalyticsSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [TextAnalyticsConnectorConfig.TopicsConfig] = "test-topic",
            [TextAnalyticsConnectorConfig.EndpointConfig] = "https://test.cognitiveservices.azure.com"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][TextAnalyticsConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void TextAnalyticsSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new TextAnalyticsSinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == TextAnalyticsConnectorConfig.ModeConfig);
        Assert.Equal(TextAnalyticsConnectorConfig.ModeSentiment, modeKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == TextAnalyticsConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)TextAnalyticsConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var languageKey = config.Keys.First(k => k.Name == TextAnalyticsConnectorConfig.LanguageConfig);
        Assert.Equal(TextAnalyticsConnectorConfig.DefaultLanguage, languageKey.DefaultValue);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasValidModeConstants()
    {
        Assert.Equal("sentiment", TextAnalyticsConnectorConfig.ModeSentiment);
        Assert.Equal("entities", TextAnalyticsConnectorConfig.ModeEntities);
        Assert.Equal("key-phrases", TextAnalyticsConnectorConfig.ModeKeyPhrases);
        Assert.Equal("language-detection", TextAnalyticsConnectorConfig.ModeLanguageDetection);
        Assert.Equal("pii", TextAnalyticsConnectorConfig.ModePii);
        Assert.Equal("linked-entities", TextAnalyticsConnectorConfig.ModeLinkedEntities);
        Assert.Equal("healthcare", TextAnalyticsConnectorConfig.ModeHealthcare);
        Assert.Equal("summarization", TextAnalyticsConnectorConfig.ModeSummarization);
        Assert.Equal("abstractive-summarization", TextAnalyticsConnectorConfig.ModeAbstractiveSummarization);
    }

    [Fact]
    public void TextAnalyticsConnectorConfig_HasCorrectDefaults()
    {
        Assert.Equal("text", TextAnalyticsConnectorConfig.DefaultInputField);
        Assert.Equal("result", TextAnalyticsConnectorConfig.DefaultOutputField);
        Assert.Equal("en", TextAnalyticsConnectorConfig.DefaultLanguage);
        Assert.Equal(10, TextAnalyticsConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, TextAnalyticsConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, TextAnalyticsConnectorConfig.DefaultMaxSentenceCount);
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
