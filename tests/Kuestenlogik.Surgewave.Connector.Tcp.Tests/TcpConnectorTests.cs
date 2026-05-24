using System.Net;
using System.Net.Sockets;
using System.Text;
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Tcp;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Tcp.Tests;

/// <summary>
/// Tests for TCP source and sink connectors.
/// </summary>
public sealed class TcpConnectorTests
{
    [Fact]
    public void TcpConnectorConfig_HasExpectedConstants()
    {
        // Common
        Assert.Equal("host", TcpConnectorConfig.Host);
        Assert.Equal("port", TcpConnectorConfig.Port);
        Assert.Equal("topic", TcpConnectorConfig.Topic);
        Assert.Equal("topics", TcpConnectorConfig.Topics);

        // TLS
        Assert.Equal("tls.enabled", TcpConnectorConfig.UseTls);
        Assert.False(TcpConnectorConfig.DefaultUseTls);

        // Source
        Assert.Equal("listen.address", TcpConnectorConfig.ListenAddress);
        Assert.Equal("0.0.0.0", TcpConnectorConfig.DefaultListenAddress);
        Assert.Equal("listen.port", TcpConnectorConfig.ListenPort);
        Assert.Equal(9999, TcpConnectorConfig.DefaultListenPort);
        Assert.Equal("max.connections", TcpConnectorConfig.MaxConnections);
        Assert.Equal(100, TcpConnectorConfig.DefaultMaxConnections);

        // Framing
        Assert.Equal("framing", TcpConnectorConfig.Framing);
        Assert.Equal("line", TcpConnectorConfig.FramingLine);
        Assert.Equal("length-prefix", TcpConnectorConfig.FramingLengthPrefix);
        Assert.Equal("delimiter", TcpConnectorConfig.FramingDelimiter);
        Assert.Equal("line", TcpConnectorConfig.DefaultFraming);
        Assert.Equal("delimiter.bytes", TcpConnectorConfig.Delimiter);
        Assert.Equal("length.prefix.bytes", TcpConnectorConfig.LengthPrefixBytes);
        Assert.Equal(4, TcpConnectorConfig.DefaultLengthPrefixBytes);
        Assert.True(TcpConnectorConfig.DefaultLengthPrefixBigEndian);
        Assert.Equal(1048576, TcpConnectorConfig.DefaultMaxMessageSize);

        // Sink
        Assert.True(TcpConnectorConfig.DefaultReconnect);
        Assert.Equal(1000, TcpConnectorConfig.DefaultReconnectDelayMs);
        Assert.Equal(30000, TcpConnectorConfig.DefaultReconnectMaxDelayMs);
    }

    [Fact]
    public void TcpSourceConnector_HasCorrectConfig()
    {
        using var connector = new TcpSourceConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(TcpSourceTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.Topic);
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.ListenAddress);
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.ListenPort);
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.MaxConnections);
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.Framing);
    }

    [Fact]
    public void TcpSinkConnector_HasCorrectConfig()
    {
        using var connector = new TcpSinkConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(TcpSinkTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys;
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.Topics);
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.Host);
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.Port);
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.Framing);
        Assert.Contains(configKeys, k => k.Name == TcpConnectorConfig.Reconnect);
    }

    [Fact]
    public void TcpSourceConnector_ThrowsOnMissingTopic()
    {
        using var connector = new TcpSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TcpSinkConnector_ThrowsOnMissingConfig()
    {
        using var connector = new TcpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>();

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TcpSinkConnector_ThrowsOnMissingHost()
    {
        using var connector = new TcpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Topics] = "test-topic"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void TcpSourceConnector_ProducesTaskConfigs()
    {
        using var connector = new TcpSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Topic] = "tcp-data",
            [TcpConnectorConfig.ListenPort] = "9999"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        // TCP source supports only single task
        Assert.Single(taskConfigs);
        Assert.Equal("tcp-data", taskConfigs[0][TcpConnectorConfig.Topic]);
        Assert.Equal("9999", taskConfigs[0][TcpConnectorConfig.ListenPort]);

        connector.Stop();
    }

    [Fact]
    public void TcpSinkConnector_ProducesTaskConfigs()
    {
        using var connector = new TcpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [TcpConnectorConfig.Topics] = "topic1,topic2",
            [TcpConnectorConfig.Host] = "localhost",
            [TcpConnectorConfig.Port] = "8888",
            [TcpConnectorConfig.Framing] = TcpConnectorConfig.FramingLengthPrefix
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("topic1,topic2", taskConfigs[0][TcpConnectorConfig.Topics]);
        Assert.Equal("localhost", taskConfigs[0][TcpConnectorConfig.Host]);
        Assert.Equal("8888", taskConfigs[0][TcpConnectorConfig.Port]);

        connector.Stop();
    }
}
