namespace Kuestenlogik.Surgewave.Connector.Gcp.Language.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class LanguageSinkConnectorTests
{
    [Fact]
    public void LanguageSinkConnector_HasCorrectVersion()
    {
        using var connector = new LanguageSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void LanguageSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new LanguageSinkConnector();
        Assert.Equal(typeof(LanguageSinkTask), connector.TaskClass);
    }

    [Fact]
    public void LanguageSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new LanguageSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.ModeConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void LanguageSinkConnector_Config_HasConnectionKeys()
    {
        using var connector = new LanguageSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.ProjectIdConfig);
        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.CredentialsJsonConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.CredentialsPathConfig);
    }

    [Fact]
    public void LanguageSinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new LanguageSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.OutputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.LanguageConfig);
    }

    [Fact]
    public void LanguageSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new LanguageSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == LanguageConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void LanguageSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new LanguageSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(LanguageConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void LanguageSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new LanguageSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [LanguageConnectorConfig.TopicsConfig] = "test-topic",
            [LanguageConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void LanguageSinkConnector_Start_AcceptsValidSentimentConfig()
    {
        using var connector = new LanguageSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [LanguageConnectorConfig.TopicsConfig] = "test-topic",
            [LanguageConnectorConfig.ModeConfig] = LanguageConnectorConfig.ModeSentiment
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void LanguageSinkConnector_Start_AcceptsValidEntitiesConfig()
    {
        using var connector = new LanguageSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [LanguageConnectorConfig.TopicsConfig] = "test-topic",
            [LanguageConnectorConfig.ModeConfig] = LanguageConnectorConfig.ModeEntities
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void LanguageSinkConnector_Start_AcceptsValidSyntaxConfig()
    {
        using var connector = new LanguageSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [LanguageConnectorConfig.TopicsConfig] = "test-topic",
            [LanguageConnectorConfig.ModeConfig] = LanguageConnectorConfig.ModeSyntax
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void LanguageSinkConnector_Start_AcceptsValidClassifyConfig()
    {
        using var connector = new LanguageSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [LanguageConnectorConfig.TopicsConfig] = "test-topic",
            [LanguageConnectorConfig.ModeConfig] = LanguageConnectorConfig.ModeClassify
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void LanguageSinkConnector_Start_AcceptsValidAllConfig()
    {
        using var connector = new LanguageSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [LanguageConnectorConfig.TopicsConfig] = "test-topic",
            [LanguageConnectorConfig.ModeConfig] = LanguageConnectorConfig.ModeAll
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void LanguageSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new LanguageSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [LanguageConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][LanguageConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void LanguageSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new LanguageSinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == LanguageConnectorConfig.ModeConfig);
        Assert.Equal(LanguageConnectorConfig.ModeSentiment, modeKey.DefaultValue);

        var languageKey = config.Keys.First(k => k.Name == LanguageConnectorConfig.LanguageConfig);
        Assert.Equal(LanguageConnectorConfig.DefaultLanguage, languageKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == LanguageConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)LanguageConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);
    }

    [Fact]
    public void LanguageConnectorConfig_HasValidModeConstants()
    {
        Assert.Equal("sentiment", LanguageConnectorConfig.ModeSentiment);
        Assert.Equal("entities", LanguageConnectorConfig.ModeEntities);
        Assert.Equal("syntax", LanguageConnectorConfig.ModeSyntax);
        Assert.Equal("classify", LanguageConnectorConfig.ModeClassify);
        Assert.Equal("all", LanguageConnectorConfig.ModeAll);
    }

    [Fact]
    public void LanguageConnectorConfig_HasCorrectDefaults()
    {
        Assert.Equal("en", LanguageConnectorConfig.DefaultLanguage);
        Assert.Equal("text", LanguageConnectorConfig.DefaultInputField);
        Assert.Equal("analysis", LanguageConnectorConfig.DefaultOutputField);
        Assert.Equal(10, LanguageConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, LanguageConnectorConfig.DefaultBatchTimeoutMs);
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
