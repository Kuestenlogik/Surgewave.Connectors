namespace Kuestenlogik.Surgewave.Connector.Aws.Bedrock.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class BedrockSinkConnectorTests
{
    [Fact]
    public void BedrockSinkConnector_HasCorrectVersion()
    {
        using var connector = new BedrockSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void BedrockSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new BedrockSinkConnector();
        Assert.Equal(typeof(BedrockSinkTask), connector.TaskClass);
    }

    [Fact]
    public void BedrockSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new BedrockSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.TopicsConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.ModeConfig && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.ModelIdConfig && k.Type == ConfigType.String);
    }

    [Fact]
    public void BedrockSinkConnector_Config_HasConnectionKeys()
    {
        using var connector = new BedrockSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.RegionConfig);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.AccessKeyConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.SecretKeyConfig && k.Type == ConfigType.Password);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.EndpointConfig);
    }

    [Fact]
    public void BedrockSinkConnector_Config_HasInputOutputKeys()
    {
        using var connector = new BedrockSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.InputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.OutputFieldConfig);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.EmbeddingsFieldConfig);
    }

    [Fact]
    public void BedrockSinkConnector_Config_HasCompletionKeys()
    {
        using var connector = new BedrockSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.SystemPromptConfig);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.MaxTokensConfig);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.TemperatureConfig);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.TopPConfig);
    }

    [Fact]
    public void BedrockSinkConnector_Config_HasBatchingKeys()
    {
        using var connector = new BedrockSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.BatchSizeConfig);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.BatchTimeoutMsConfig);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.RetryMaxConfig);
        Assert.Contains(config.Keys, k => k.Name == BedrockConnectorConfig.RetryBackoffMsConfig);
    }

    [Fact]
    public void BedrockSinkConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new BedrockSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>();

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains(BedrockConnectorConfig.TopicsConfig, ex.Message);
    }

    [Fact]
    public void BedrockSinkConnector_Start_ThrowsOnInvalidMode()
    {
        using var connector = new BedrockSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [BedrockConnectorConfig.TopicsConfig] = "test-topic",
            [BedrockConnectorConfig.ModeConfig] = "invalid"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("Invalid mode", ex.Message);
    }

    [Fact]
    public void BedrockSinkConnector_Start_AcceptsValidChatConfig()
    {
        using var connector = new BedrockSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [BedrockConnectorConfig.TopicsConfig] = "test-topic",
            [BedrockConnectorConfig.ModeConfig] = BedrockConnectorConfig.ModeChat
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void BedrockSinkConnector_Start_AcceptsValidEmbeddingsConfig()
    {
        using var connector = new BedrockSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [BedrockConnectorConfig.TopicsConfig] = "test-topic",
            [BedrockConnectorConfig.ModeConfig] = BedrockConnectorConfig.ModeEmbeddings
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void BedrockSinkConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new BedrockSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            [BedrockConnectorConfig.TopicsConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("test-topic", taskConfigs[0][BedrockConnectorConfig.TopicsConfig]);
    }

    [Fact]
    public void BedrockSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new BedrockSinkConnector();
        var config = connector.Config;

        var modeKey = config.Keys.First(k => k.Name == BedrockConnectorConfig.ModeConfig);
        Assert.Equal(BedrockConnectorConfig.ModeChat, modeKey.DefaultValue);

        var modelKey = config.Keys.First(k => k.Name == BedrockConnectorConfig.ModelIdConfig);
        Assert.Equal(BedrockConnectorConfig.DefaultModelId, modelKey.DefaultValue);

        var batchSizeKey = config.Keys.First(k => k.Name == BedrockConnectorConfig.BatchSizeConfig);
        Assert.Equal((long)BedrockConnectorConfig.DefaultBatchSize, batchSizeKey.DefaultValue);

        var regionKey = config.Keys.First(k => k.Name == BedrockConnectorConfig.RegionConfig);
        Assert.Equal("us-east-1", regionKey.DefaultValue);
    }

    [Fact]
    public void BedrockConnectorConfig_HasValidModeConstants()
    {
        Assert.Equal("chat", BedrockConnectorConfig.ModeChat);
        Assert.Equal("embeddings", BedrockConnectorConfig.ModeEmbeddings);
    }

    [Fact]
    public void BedrockConnectorConfig_HasCorrectDefaults()
    {
        Assert.Equal("text", BedrockConnectorConfig.DefaultInputField);
        Assert.Equal("response", BedrockConnectorConfig.DefaultOutputField);
        Assert.Equal("embedding", BedrockConnectorConfig.DefaultEmbeddingsField);
        Assert.Equal(10, BedrockConnectorConfig.DefaultBatchSize);
        Assert.Equal(5000, BedrockConnectorConfig.DefaultBatchTimeoutMs);
        Assert.Equal(4096, BedrockConnectorConfig.DefaultMaxTokens);
        Assert.Equal(0.7, BedrockConnectorConfig.DefaultTemperature);
    }

    [Fact]
    public void BedrockConnectorConfig_HasValidModelIds()
    {
        Assert.NotEmpty(BedrockConnectorConfig.ModelClaudeSonnet35);
        Assert.NotEmpty(BedrockConnectorConfig.ModelClaudeHaiku35);
        Assert.NotEmpty(BedrockConnectorConfig.ModelClaudeOpus);
        Assert.NotEmpty(BedrockConnectorConfig.ModelLlama370B);
        Assert.NotEmpty(BedrockConnectorConfig.ModelLlama38B);
        Assert.NotEmpty(BedrockConnectorConfig.ModelTitanText);
        Assert.NotEmpty(BedrockConnectorConfig.ModelTitanEmbed);
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
