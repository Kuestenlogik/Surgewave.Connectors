using Xunit;
using Kuestenlogik.Surgewave.Connector.Sequence;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Sequence.Tests;

public class SequenceSourceConnectorTests
{
    [Fact]
    public void Version_ReturnsExpectedVersion()
    {
        using var connector = new SequenceSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsSequenceSourceTask()
    {
        using var connector = new SequenceSourceConnector();
        Assert.Equal(typeof(SequenceSourceTask), connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsSourcesConfig()
    {
        using var connector = new SequenceSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == SequenceConnectorConfig.SourcesConfig);
    }

    [Fact]
    public void Config_ContainsTopicConfig()
    {
        using var connector = new SequenceSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == SequenceConnectorConfig.TopicConfig);
    }

    [Fact]
    public void Config_ContainsContinueOnErrorConfig()
    {
        using var connector = new SequenceSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == SequenceConnectorConfig.ContinueOnErrorConfig);
    }

    [Fact]
    public void Config_ContainsEmptyPollsBeforeAdvanceConfig()
    {
        using var connector = new SequenceSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig);
    }

    [Fact]
    public void Config_ContainsEmptyPollDelayMsConfig()
    {
        using var connector = new SequenceSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == SequenceConnectorConfig.EmptyPollDelayMsConfig);
    }

    [Fact]
    public void Config_ContainsIncludeSourceIndexConfig()
    {
        using var connector = new SequenceSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == SequenceConnectorConfig.IncludeSourceIndexConfig);
    }

    [Fact]
    public void Config_ContainsSourceIndexHeaderConfig()
    {
        using var connector = new SequenceSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == SequenceConnectorConfig.SourceIndexHeaderConfig);
    }

    [Fact]
    public void Config_ContainsCompletionBehaviorConfig()
    {
        using var connector = new SequenceSourceConnector();
        var config = connector.Config;
        Assert.Contains(config.Keys, k => k.Name == SequenceConnectorConfig.CompletionBehaviorConfig);
    }

    [Fact]
    public void Start_WithValidConfig_Succeeds()
    {
        using var connector = new SequenceSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "[]",
            [SequenceConnectorConfig.TopicConfig] = "test-topic"
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithMissingSources_ThrowsArgumentException()
    {
        using var connector = new SequenceSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.TopicConfig] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithMissingTopic_ThrowsArgumentException()
    {
        using var connector = new SequenceSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "[]"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithEmptySources_ThrowsArgumentException()
    {
        using var connector = new SequenceSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "",
            [SequenceConnectorConfig.TopicConfig] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithInvalidCompletionBehavior_ThrowsArgumentException()
    {
        using var connector = new SequenceSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "[]",
            [SequenceConnectorConfig.TopicConfig] = "test-topic",
            [SequenceConnectorConfig.CompletionBehaviorConfig] = "invalid"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void Start_WithStopBehavior_Succeeds()
    {
        using var connector = new SequenceSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "[]",
            [SequenceConnectorConfig.TopicConfig] = "test-topic",
            [SequenceConnectorConfig.CompletionBehaviorConfig] = SequenceConnectorConfig.CompletionBehaviorStop
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void Start_WithRestartBehavior_Succeeds()
    {
        using var connector = new SequenceSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "[]",
            [SequenceConnectorConfig.TopicConfig] = "test-topic",
            [SequenceConnectorConfig.CompletionBehaviorConfig] = SequenceConnectorConfig.CompletionBehaviorRestart
        };

        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        using var connector = new SequenceSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "[]",
            [SequenceConnectorConfig.TopicConfig] = "test-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(10);

        // Sequence connector always uses single task to maintain ordering
        Assert.Single(taskConfigs);
        Assert.Equal("[]", taskConfigs[0][SequenceConnectorConfig.SourcesConfig]);
        Assert.Equal("test-topic", taskConfigs[0][SequenceConnectorConfig.TopicConfig]);

        connector.Stop();
    }

    [Fact]
    public void TaskConfigs_PreservesAllConfigValues()
    {
        using var connector = new SequenceSourceConnector();
        var context = new ConnectorContext();
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [SequenceConnectorConfig.SourcesConfig] = "[]",
            [SequenceConnectorConfig.TopicConfig] = "test-topic",
            [SequenceConnectorConfig.ContinueOnErrorConfig] = "true",
            [SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig] = "5"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Single(taskConfigs);
        Assert.Equal("true", taskConfigs[0][SequenceConnectorConfig.ContinueOnErrorConfig]);
        Assert.Equal("5", taskConfigs[0][SequenceConnectorConfig.EmptyPollsBeforeAdvanceConfig]);

        connector.Stop();
    }
}
