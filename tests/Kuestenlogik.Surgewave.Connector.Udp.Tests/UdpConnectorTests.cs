using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Connector.Udp;
using Xunit;

namespace Kuestenlogik.Surgewave.Connector.Udp.Tests;

/// <summary>
/// Tests for UDP source and sink connectors.
/// </summary>
public sealed class UdpConnectorTests
{
    [Fact]
    public void UdpConnectorConfig_HasExpectedConstants()
    {
        // Topics
        Assert.Equal("topic", UdpConnectorConfig.Topic);
        Assert.Equal("topics", UdpConnectorConfig.Topics);

        // Source config
        Assert.Equal("listen.address", UdpConnectorConfig.ListenAddress);
        Assert.Equal("0.0.0.0", UdpConnectorConfig.DefaultListenAddress);
        Assert.Equal("listen.port", UdpConnectorConfig.ListenPort);
        Assert.Equal(9999, UdpConnectorConfig.DefaultListenPort);

        // Sink config
        Assert.Equal("host", UdpConnectorConfig.Host);
        Assert.Equal("port", UdpConnectorConfig.Port);

        // Multicast
        Assert.Equal("multicast.enabled", UdpConnectorConfig.MulticastEnabled);
        Assert.False(UdpConnectorConfig.DefaultMulticastEnabled);
        Assert.Equal("multicast.group", UdpConnectorConfig.MulticastGroup);
        Assert.Equal("multicast.ttl", UdpConnectorConfig.MulticastTtl);
        Assert.Equal(1, UdpConnectorConfig.DefaultMulticastTtl);
        Assert.Equal("multicast.loopback", UdpConnectorConfig.MulticastLoopback);
        Assert.False(UdpConnectorConfig.DefaultMulticastLoopback);

        // Message settings
        Assert.Equal(65507, UdpConnectorConfig.DefaultMaxMessageSize);
        Assert.True(UdpConnectorConfig.DefaultIncludeSourceInfo);
    }

    [Fact]
    public void UdpSourceConnector_HasCorrectConfig()
    {
        using var connector = new UdpSourceConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(UdpSourceTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys.Select(k => k.Name).ToList();
        Assert.Contains(UdpConnectorConfig.Topic, configKeys);
        Assert.Contains(UdpConnectorConfig.ListenAddress, configKeys);
        Assert.Contains(UdpConnectorConfig.ListenPort, configKeys);
        Assert.Contains(UdpConnectorConfig.MulticastEnabled, configKeys);
    }

    [Fact]
    public void UdpSinkConnector_HasCorrectConfig()
    {
        using var connector = new UdpSinkConnector();

        Assert.Equal("1.0.0", connector.Version);
        Assert.Equal(typeof(UdpSinkTask), connector.TaskClass);
        Assert.NotNull(connector.Config);

        var configKeys = connector.Config.Keys.Select(k => k.Name).ToList();
        Assert.Contains(UdpConnectorConfig.Topics, configKeys);
        Assert.Contains(UdpConnectorConfig.Host, configKeys);
        Assert.Contains(UdpConnectorConfig.Port, configKeys);
        Assert.Contains(UdpConnectorConfig.MulticastEnabled, configKeys);
    }

    [Fact]
    public void UdpSourceConnector_ThrowsOnMissingTopic()
    {
        using var connector = new UdpSourceConnector();
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
    public void UdpSinkConnector_ThrowsOnMissingTopics()
    {
        using var connector = new UdpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Host] = "localhost",
            [UdpConnectorConfig.Port] = "9999"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void UdpSinkConnector_ThrowsOnMissingHost()
    {
        using var connector = new UdpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Topics] = "test",
            [UdpConnectorConfig.Port] = "9999"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void UdpSinkConnector_ThrowsOnMissingPort()
    {
        using var connector = new UdpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Topics] = "test",
            [UdpConnectorConfig.Host] = "localhost"
        };

        Assert.Throws<ArgumentException>(() => connector.Start(config));
    }

    [Fact]
    public void UdpSourceConnector_ProducesTaskConfigs()
    {
        using var connector = new UdpSourceConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Topic] = "udp-data",
            [UdpConnectorConfig.ListenPort] = "8888"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("udp-data", taskConfigs[0][UdpConnectorConfig.Topic]);
        Assert.Equal("8888", taskConfigs[0][UdpConnectorConfig.ListenPort]);

        connector.Stop();
    }

    [Fact]
    public void UdpSinkConnector_ProducesTaskConfigs()
    {
        using var connector = new UdpSinkConnector();
        var context = new ConnectorContext
        {
            RequestTaskReconfiguration = () => { },
            RaiseError = _ => { }
        };
        connector.Initialize(context);

        var config = new Dictionary<string, string>
        {
            [UdpConnectorConfig.Topics] = "topic1,topic2",
            [UdpConnectorConfig.Host] = "192.168.1.100",
            [UdpConnectorConfig.Port] = "5000"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Single(taskConfigs);
        Assert.Equal("topic1,topic2", taskConfigs[0][UdpConnectorConfig.Topics]);
        Assert.Equal("192.168.1.100", taskConfigs[0][UdpConnectorConfig.Host]);
        Assert.Equal("5000", taskConfigs[0][UdpConnectorConfig.Port]);

        connector.Stop();
    }
}
