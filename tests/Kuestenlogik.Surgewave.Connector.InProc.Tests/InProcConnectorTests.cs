using Kuestenlogik.Surgewave.Connector.InProc;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.InProc.Tests;

/// <summary>
/// Tests for InProc source and sink connectors.
/// </summary>
public sealed class InProcConnectorTests
{
    [Fact]
    public void InProcSourceConnector_HasCorrectVersion()
    {
        var connector = new InProcSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void InProcSinkConnector_HasCorrectVersion()
    {
        var connector = new InProcSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void InProcConnectorConfig_HasExpectedConstants()
    {
        Assert.Equal("topic", InProcConnectorConfig.Topic);
        Assert.Equal("topics", InProcConnectorConfig.Topics);
        Assert.Equal("channel.name", InProcConnectorConfig.ChannelName);
        Assert.Equal("mode", InProcConnectorConfig.Mode);
        Assert.Equal("buffer.size", InProcConnectorConfig.BufferSize);
        Assert.Equal("pipe.name", InProcConnectorConfig.PipeName);
        Assert.Equal("sharedmemory.name", InProcConnectorConfig.SharedMemoryName);
        Assert.Equal("channel", InProcConnectorConfig.ModeChannel);
        Assert.Equal("namedpipe", InProcConnectorConfig.ModeNamedPipe);
        Assert.Equal("sharedmemory", InProcConnectorConfig.ModeSharedMemory);
    }

    [Fact]
    public void InProcSourceConnector_ThrowsOnMissingTopic()
    {
        var connector = new InProcSourceConnector();
        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void InProcSourceConnector_ThrowsOnMissingChannelName()
    {
        var connector = new InProcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void InProcSourceConnector_ThrowsOnMissingPipeName()
    {
        var connector = new InProcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeNamedPipe
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void InProcSourceConnector_ThrowsOnMissingSharedMemoryName()
    {
        var connector = new InProcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeSharedMemory
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void InProcSourceConnector_ThrowsOnInvalidMode()
    {
        var connector = new InProcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test",
            [InProcConnectorConfig.Mode] = "invalid"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void InProcSourceConnector_ProducesTaskConfigsForChannel()
    {
        var connector = new InProcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test-topic",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = "test-channel"
        };

        connector.Start(config);
        var configs = connector.TaskConfigs(1);

        Assert.Single(configs);
        Assert.Equal("test-topic", configs[0][InProcConnectorConfig.Topic]);
        Assert.Equal(InProcConnectorConfig.ModeChannel, configs[0][InProcConnectorConfig.Mode]);
        Assert.Equal("test-channel", configs[0][InProcConnectorConfig.ChannelName]);
    }

    [Fact]
    public void InProcSinkConnector_ThrowsOnMissingTopics()
    {
        var connector = new InProcSinkConnector();
        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void InProcSinkConnector_ThrowsOnMissingChannelName()
    {
        var connector = new InProcSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void InProcSinkConnector_ProducesTaskConfigsForChannel()
    {
        var connector = new InProcSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "topic1,topic2",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = "sink-channel"
        };

        connector.Start(config);
        var configs = connector.TaskConfigs(1);

        Assert.Single(configs);
        Assert.Equal("topic1,topic2", configs[0][InProcConnectorConfig.Topics]);
        Assert.Equal(InProcConnectorConfig.ModeChannel, configs[0][InProcConnectorConfig.Mode]);
        Assert.Equal("sink-channel", configs[0][InProcConnectorConfig.ChannelName]);
    }

    [Fact]
    public void InProcSourceConnector_StopsCleanly()
    {
        var connector = new InProcSourceConnector();
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topic] = "test-topic",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = "stop-test"
        };

        connector.Start(config);
        connector.Stop();
        // Should not throw
    }

    [Fact]
    public void InProcSinkConnector_StopsCleanly()
    {
        var connector = new InProcSinkConnector();
        var config = new Dictionary<string, string>
        {
            [InProcConnectorConfig.Topics] = "test",
            [InProcConnectorConfig.Mode] = InProcConnectorConfig.ModeChannel,
            [InProcConnectorConfig.ChannelName] = "stop-test"
        };

        connector.Start(config);
        connector.Stop();
        // Should not throw
    }

    [Fact]
    public void InProcSourceConnector_HasCorrectTaskClass()
    {
        var connector = new InProcSourceConnector();
        Assert.Equal(typeof(InProcSourceTask), connector.TaskClass);
    }

    [Fact]
    public void InProcSinkConnector_HasCorrectTaskClass()
    {
        var connector = new InProcSinkConnector();
        Assert.Equal(typeof(InProcSinkTask), connector.TaskClass);
    }

    [Fact]
    public void InProcSourceConnector_HasConfig()
    {
        var connector = new InProcSourceConnector();
        var configDef = connector.Config;
        Assert.NotNull(configDef);
        Assert.True(configDef.Keys.Count > 0);
    }

    [Fact]
    public void InProcSinkConnector_HasConfig()
    {
        var connector = new InProcSinkConnector();
        var configDef = connector.Config;
        Assert.NotNull(configDef);
        Assert.True(configDef.Keys.Count > 0);
    }
}
