namespace Kuestenlogik.Surgewave.Connector.Aws.Comprehend.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class ComprehendSinkConnectorTests
{
    [Fact]
    public void ComprehendSinkConnector_HasCorrectVersion()
    {
        using var connector = new ComprehendSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void ComprehendSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new ComprehendSinkConnector();
        Assert.Equal(typeof(ComprehendSinkTask), connector.TaskClass);
    }

    [Fact]
    public void ComprehendSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new ComprehendSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void ComprehendSinkConnector_Config_HasConnectionKeys()
    {
        using var connector = new ComprehendSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.RegionConfig);
        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.AccessKeyConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.SecretKeyConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void ComprehendSinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new ComprehendSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.OutputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.LanguageConfig);
    }

    [Fact]
    public void ComprehendSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new ComprehendSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == ComprehendConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void ComprehendSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(ComprehendConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void ComprehendSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [ComprehendConnectorConfig.TopicsConfig] = "test-topic",
            [ComprehendConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void ComprehendSinkConnector_Start_AcceptsValidSentimentConfig()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [ComprehendConnectorConfig.TopicsConfig] = "test-topic",
            [ComprehendConnectorConfig.ModeConfig] = ComprehendConnectorConfig.ModeSentiment
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ComprehendSinkConnector_Start_AcceptsValidEntitiesConfig()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [ComprehendConnectorConfig.TopicsConfig] = "test-topic",
            [ComprehendConnectorConfig.ModeConfig] = ComprehendConnectorConfig.ModeEntities
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ComprehendSinkConnector_Start_AcceptsValidKeyPhrasesConfig()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [ComprehendConnectorConfig.TopicsConfig] = "test-topic",
            [ComprehendConnectorConfig.ModeConfig] = ComprehendConnectorConfig.ModeKeyPhrases
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ComprehendSinkConnector_Start_AcceptsValidLanguageConfig()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [ComprehendConnectorConfig.TopicsConfig] = "test-topic",
            [ComprehendConnectorConfig.ModeConfig] = ComprehendConnectorConfig.ModeLanguage
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ComprehendSinkConnector_Start_AcceptsValidPiiConfig()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [ComprehendConnectorConfig.TopicsConfig] = "test-topic",
            [ComprehendConnectorConfig.ModeConfig] = ComprehendConnectorConfig.ModePii
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ComprehendSinkConnector_Start_AcceptsValidSyntaxConfig()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [ComprehendConnectorConfig.TopicsConfig] = "test-topic",
            [ComprehendConnectorConfig.ModeConfig] = ComprehendConnectorConfig.ModeSyntax
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ComprehendSinkConnector_Start_AcceptsValidAllConfig()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [ComprehendConnectorConfig.TopicsConfig] = "test-topic",
            [ComprehendConnectorConfig.ModeConfig] = ComprehendConnectorConfig.ModeAll
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void ComprehendSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new ComprehendSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [ComprehendConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][ComprehendConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void ComprehendSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new ComprehendSinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == ComprehendConnectorConfig.ModeConfig);
        Assert.Equal(ComprehendConnectorConfig.ModeSentiment, modeKey.DefaultValue);

        var languageKey = config.Keys.First(k => k.Name == ComprehendConnectorConfig.LanguageConfig);
        Assert.Equal(ComprehendConnectorConfig.DefaultLanguage, languageKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == ComprehendConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)ComprehendConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var regionKey = config.Keys.First(k => k.Name == ComprehendConnectorConfig.RegionConfig);
        Assert.Equal("us-east-1", regionKey.DefaultValue);
    }

    [Fact]
    public void ComprehendConnectorConfig_HasValidModeConstants()
    {
        Assert.Equal("sentiment", ComprehendConnectorConfig.ModeSentiment);
        Assert.Equal("entities", ComprehendConnectorConfig.ModeEntities);
        Assert.Equal("key_phrases", ComprehendConnectorConfig.ModeKeyPhrases);
        Assert.Equal("language", ComprehendConnectorConfig.ModeLanguage);
        Assert.Equal("pii", ComprehendConnectorConfig.ModePii);
        Assert.Equal("syntax", ComprehendConnectorConfig.ModeSyntax);
        Assert.Equal("all", ComprehendConnectorConfig.ModeAll);
    }

    [Fact]
    public void ComprehendConnectorConfig_HasCorrectDefaults()
    {
        Assert.Equal("en", ComprehendConnectorConfig.DefaultLanguage);
        Assert.Equal("text", ComprehendConnectorConfig.DefaultInputField);
        Assert.Equal("analysis", ComprehendConnectorConfig.DefaultOutputField);
        Assert.Equal(25, ComprehendConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, ComprehendConnectorConfig.DefaultBatchTimeoutMs);
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
