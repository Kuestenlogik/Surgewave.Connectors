namespace Kuestenlogik.Surgewave.Connector.DeepL.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class DeepLSinkConnectorTests
{
    [Fact]
    public void DeepLSinkConnector_HasCorrectVersion()
    {
        using var connector = new DeepLSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void DeepLSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new DeepLSinkConnector();
        Assert.Equal(typeof(DeepLSinkTask), connector.TaskClass);
    }

    [Fact]
    public void DeepLSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new DeepLSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.ApiKeyConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void DeepLSinkConnector_Config_HasTranslationKeys()
    {
        using var connector = new DeepLSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.SourceLanguageConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.TargetLanguageConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.FormalityConfig);
    }

    [Fact]
    public void DeepLSinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new DeepLSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.OutputFieldConfig);
    }

    [Fact]
    public void DeepLSinkConnector_Config_HasAdvancedKeys()
    {
        using var connector = new DeepLSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.ContextConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.GlossaryIdConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.TagHandlingConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.PreserveFormattingConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.SplitSentencesConfig);
    }

    [Fact]
    public void DeepLSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new DeepLSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == DeepLConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void DeepLSinkConnector_Start_ThrowsOnMissingApiKey()
    {
        using var connector = new DeepLSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [DeepLConnectorConfig.TopicsConfig] = "test-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(DeepLConnectorConfig.ApiKeyConfig, ex.Message);
    }

    [Fact]
    public void DeepLSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new DeepLSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [DeepLConnectorConfig.ApiKeyConfig] = "test-api-key"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(DeepLConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void DeepLSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new DeepLSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [DeepLConnectorConfig.TopicsConfig] = "test-topic",
            [DeepLConnectorConfig.ApiKeyConfig] = "test-api-key",
            [DeepLConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void DeepLSinkConnector_Start_ThrowsOnInvalidFormality()
    {
        using var connector = new DeepLSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [DeepLConnectorConfig.TopicsConfig] = "test-topic",
            [DeepLConnectorConfig.ApiKeyConfig] = "test-api-key",
            [DeepLConnectorConfig.FormalityConfig] = "super-formal"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid formality", ex.Message);
    }

    [Fact]
    public void DeepLSinkConnector_Start_AcceptsValidTranslateConfig()
    {
        using var connector = new DeepLSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [DeepLConnectorConfig.TopicsConfig] = "test-topic",
            [DeepLConnectorConfig.ApiKeyConfig] = "test-api-key",
            [DeepLConnectorConfig.ModeConfig] = DeepLConnectorConfig.ModeTranslate,
            [DeepLConnectorConfig.TargetLanguageConfig] = "DE"
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void DeepLSinkConnector_Start_AcceptsValidDetectLanguageConfig()
    {
        using var connector = new DeepLSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [DeepLConnectorConfig.TopicsConfig] = "test-topic",
            [DeepLConnectorConfig.ApiKeyConfig] = "test-api-key",
            [DeepLConnectorConfig.ModeConfig] = DeepLConnectorConfig.ModeDetectLanguage
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void DeepLSinkConnector_Start_AcceptsValidUsageConfig()
    {
        using var connector = new DeepLSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [DeepLConnectorConfig.TopicsConfig] = "test-topic",
            [DeepLConnectorConfig.ApiKeyConfig] = "test-api-key",
            [DeepLConnectorConfig.ModeConfig] = DeepLConnectorConfig.ModeUsage
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void DeepLSinkConnector_Start_AcceptsAllFormalityLevels()
    {
        var formalityLevels = new[]
        {
            DeepLConnectorConfig.FormalityDefault,
            DeepLConnectorConfig.FormalityMore,
            DeepLConnectorConfig.FormalityLess,
            DeepLConnectorConfig.FormalityPreferMore,
            DeepLConnectorConfig.FormalityPreferLess
        };

        foreach (var formality in formalityLevels)
        {
            using var connector = new DeepLSinkConnector();
            connector.Initialize(CreateContext());

            var config = new Dictionary<string, string>
            {
                [DeepLConnectorConfig.TopicsConfig] = "test-topic",
                [DeepLConnectorConfig.ApiKeyConfig] = "test-api-key",
                [DeepLConnectorConfig.FormalityConfig] = formality
            };

            // Should not throw
            connector.Start(config);
            connector.Stop();
        }
    }

    [Fact]
    public void DeepLSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new DeepLSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [DeepLConnectorConfig.TopicsConfig] = "test-topic",
            [DeepLConnectorConfig.ApiKeyConfig] = "test-api-key"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][DeepLConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void DeepLSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new DeepLSinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == DeepLConnectorConfig.ModeConfig);
        Assert.Equal(DeepLConnectorConfig.ModeTranslate, modeKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == DeepLConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)DeepLConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var targetLangKey = config.Keys.First(k => k.Name == DeepLConnectorConfig.TargetLanguageConfig);
        Assert.Equal(DeepLConnectorConfig.DefaultTargetLanguage, targetLangKey.DefaultValue);
    }

    [Fact]
    public void DeepLConnectorConfig_HasValidModeConstants()
    {
        Assert.Equal("translate", DeepLConnectorConfig.ModeTranslate);
        Assert.Equal("detect-language", DeepLConnectorConfig.ModeDetectLanguage);
        Assert.Equal("usage", DeepLConnectorConfig.ModeUsage);
    }

    [Fact]
    public void DeepLConnectorConfig_HasValidFormalityConstants()
    {
        Assert.Equal("default", DeepLConnectorConfig.FormalityDefault);
        Assert.Equal("more", DeepLConnectorConfig.FormalityMore);
        Assert.Equal("less", DeepLConnectorConfig.FormalityLess);
        Assert.Equal("prefer_more", DeepLConnectorConfig.FormalityPreferMore);
        Assert.Equal("prefer_less", DeepLConnectorConfig.FormalityPreferLess);
    }

    [Fact]
    public void DeepLConnectorConfig_HasCorrectDefaults()
    {
        Assert.Equal("text", DeepLConnectorConfig.DefaultInputField);
        Assert.Equal("result", DeepLConnectorConfig.DefaultOutputField);
        Assert.Equal("EN-US", DeepLConnectorConfig.DefaultTargetLanguage);
        Assert.Equal("", DeepLConnectorConfig.DefaultSourceLanguage);
        Assert.Equal(25, DeepLConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, DeepLConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(3, DeepLConnectorConfig.DefaultRetryMax);
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
