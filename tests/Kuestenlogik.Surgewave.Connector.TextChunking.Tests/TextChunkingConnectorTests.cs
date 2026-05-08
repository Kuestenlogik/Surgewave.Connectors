using Kuestenlogik.Surgewave.Connector.TextChunking;
using Kuestenlogik.Surgewave.Connect;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.TextChunking.Tests;

/// <summary>
/// Tests for the TextChunkingConnector class.
/// </summary>
public sealed class TextChunkingConnectorTests : IDisposable
{
    private readonly TextChunkingConnector _connector = new();

    public void Dispose()
    {
        _connector.Dispose();
    }

    [Fact]
    public void Version_ReturnsExpected()
    {
        Assert.Equal("1.0.0", _connector.Version);
    }

    [Fact]
    public void TaskClass_ReturnsTextChunkingTask()
    {
        Assert.Equal(typeof(TextChunkingTask), _connector.TaskClass);
    }

    [Fact]
    public void Config_ContainsRequiredKeys()
    {
        var configKeys = _connector.Config.Keys.Select(k => k.Name).ToList();

        Assert.Contains(TextChunkingConfig.Topics, configKeys);
        Assert.Contains(TextChunkingConfig.OutputTopic, configKeys);
        Assert.Contains(TextChunkingConfig.Strategy, configKeys);
        Assert.Contains(TextChunkingConfig.ChunkSize, configKeys);
        Assert.Contains(TextChunkingConfig.ChunkOverlap, configKeys);
    }

    [Fact]
    public void Start_ThrowsOnMissingTopics()
    {
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        _connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.OutputTopic] = "output-topic"
        };

        Assert.Throws<ArgumentException>(() => _connector.Start(config));
    }

    [Fact]
    public void Start_ThrowsOnMissingOutputTopic()
    {
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        _connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.Topics] = "input-topic"
        };

        Assert.Throws<ArgumentException>(() => _connector.Start(config));
    }

    [Fact]
    public void Start_SucceedsWithValidConfig()
    {
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        _connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.Topics] = "input-topic",
            [TextChunkingConfig.OutputTopic] = "output-topic"
        };

        _connector.Start(config); // Should not throw
        _connector.Stop();
    }

    [Fact]
    public void TaskConfigs_ReturnsSingleConfig()
    {
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        _connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.Topics] = "input-topic",
            [TextChunkingConfig.OutputTopic] = "output-topic"
        };

        _connector.Start(config);
        var taskConfigs = _connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("input-topic", taskConfigs[0][TextChunkingConfig.Topics]);
        Assert.Equal("output-topic", taskConfigs[0][TextChunkingConfig.OutputTopic]);

        _connector.Stop();
    }

    [Fact]
    public void TaskConfigs_IncludesAllConfigValues()
    {
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        _connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TextChunkingConfig.Topics] = "input-topic",
            [TextChunkingConfig.OutputTopic] = "output-topic",
            [TextChunkingConfig.Strategy] = "sentence",
            [TextChunkingConfig.ChunkSize] = "500",
            [TextChunkingConfig.ChunkOverlap] = "50"
        };

        _connector.Start(config);
        var taskConfigs = _connector.TaskConfigs(1);

        Assert.Equal("sentence", taskConfigs[0][TextChunkingConfig.Strategy]);
        Assert.Equal("500", taskConfigs[0][TextChunkingConfig.ChunkSize]);
        Assert.Equal("50", taskConfigs[0][TextChunkingConfig.ChunkOverlap]);

        _connector.Stop();
    }
}
