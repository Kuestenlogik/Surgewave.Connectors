using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Script;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Script.Tests;

/// <summary>
/// Tests for Script transform connector.
/// </summary>
public sealed class ScriptConnectorTests
{
    [Fact]
    public void ScriptConnectorConfig_HasExpectedConstants()
    {
        Assert.Equal("topics", ScriptConnectorConfig.Topics);
        Assert.Equal("output.topic", ScriptConnectorConfig.OutputTopic);
        Assert.Equal("script.path", ScriptConnectorConfig.ScriptPath);
        Assert.Equal("script.inline", ScriptConnectorConfig.ScriptInline);
        Assert.Equal("script.language", ScriptConnectorConfig.ScriptLanguage);
        Assert.Equal("csharp", ScriptConnectorConfig.DefaultScriptLanguage);
        Assert.Equal("timeout.ms", ScriptConnectorConfig.TimeoutMs);
        Assert.Equal(30000, ScriptConnectorConfig.DefaultTimeoutMs);
        Assert.Equal("error.handling", ScriptConnectorConfig.ErrorHandling);
        Assert.Equal("skip", ScriptConnectorConfig.DefaultErrorHandling);
        Assert.Equal("dead.letter.topic", ScriptConnectorConfig.DeadLetterTopic);
        Assert.Equal("batch.size", ScriptConnectorConfig.BatchSize);
        Assert.Equal(1, ScriptConnectorConfig.DefaultBatchSize);
        Assert.Equal("process.mode", ScriptConnectorConfig.ProcessMode);
        Assert.Equal("record", ScriptConnectorConfig.DefaultProcessMode);
    }

    [Fact]
    public void ScriptTransformConnector_HasCorrectConfig()
    {
        using var connector = new ScriptTransformConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(ScriptTransformTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == ScriptConnectorConfig.Topics);
        Assert.Contains(configKeys, k => k.Name == ScriptConnectorConfig.OutputTopic);
        Assert.Contains(configKeys, k => k.Name == ScriptConnectorConfig.ScriptPath);
        Assert.Contains(configKeys, k => k.Name == ScriptConnectorConfig.ScriptInline);
        Assert.Contains(configKeys, k => k.Name == ScriptConnectorConfig.TimeoutMs);
        Assert.Contains(configKeys, k => k.Name == ScriptConnectorConfig.ErrorHandling);
    }

    [Fact]
    public void ScriptTransformConnector_ThrowsOnMissingTopics()
    {
        using var connector = new ScriptTransformConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.OutputTopic] = "output",
            [ScriptConnectorConfig.ScriptInline] = "result.Skip = true;"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ScriptTransformConnector_ThrowsOnMissingOutputTopic()
    {
        using var connector = new ScriptTransformConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.Topics] = "input",
            [ScriptConnectorConfig.ScriptInline] = "result.Skip = true;"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ScriptTransformConnector_ThrowsOnMissingScript()
    {
        using var connector = new ScriptTransformConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.Topics] = "input",
            [ScriptConnectorConfig.OutputTopic] = "output"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void ScriptTransformConnector_ProducesTaskConfigs()
    {
        using var connector = new ScriptTransformConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [ScriptConnectorConfig.Topics] = "input-topic",
            [ScriptConnectorConfig.OutputTopic] = "output-topic",
            [ScriptConnectorConfig.ScriptInline] = "result.Skip = true;"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("input-topic", taskConfigs[0][ScriptConnectorConfig.Topics]);
        Assert.Equal("output-topic", taskConfigs[0][ScriptConnectorConfig.OutputTopic]);

        connector.Stop();
    }
}
