namespace Kuestenlogik.Surgewave.Connector.Amqp.Tests;

/// <summary>
/// Tests for the connection factory both AMQP tasks build from their configuration.
/// </summary>
public class AmqpConnectionFactoryTests
{
    [Fact]
    public void CreateConnectionFactory_MapsTheDiscreteConnectionSettings()
    {
        var factory = AmqpSourceTask.CreateConnectionFactory(new Dictionary<string, string>
        {
            [AmqpConnectorConfig.Host] = "broker.internal",
            [AmqpConnectorConfig.Port] = "5680",
            [AmqpConnectorConfig.VirtualHost] = "/prod",
            [AmqpConnectorConfig.Username] = "svc",
            [AmqpConnectorConfig.Password] = "secret",
            [AmqpConnectorConfig.RequestedHeartbeat] = "17"
        });

        Assert.Equal("broker.internal", factory.HostName);
        Assert.Equal(5680, factory.Port);
        Assert.Equal("/prod", factory.VirtualHost);
        Assert.Equal("svc", factory.UserName);
        Assert.Equal("secret", factory.Password);
        // 'amqp.heartbeat.seconds' is read here, so both connectors have to declare it.
        Assert.Equal(TimeSpan.FromSeconds(17), factory.RequestedHeartbeat);
    }

    [Fact]
    public void CreateConnectionFactory_AppliesTheDefaultsWhenNothingIsConfigured()
    {
        var factory = AmqpSinkTask.CreateConnectionFactory(new Dictionary<string, string>());

        Assert.Equal(TimeSpan.FromSeconds(AmqpConnectorConfig.DefaultHeartbeatSeconds), factory.RequestedHeartbeat);
        Assert.Equal("localhost", factory.HostName);
        Assert.Equal(AmqpConnectorConfig.DefaultPort, factory.Port);
    }

    [Fact]
    public void CreateConnectionFactory_SwitchesToTheTlsPortWhenSslIsEnabled()
    {
        var factory = AmqpSinkTask.CreateConnectionFactory(new Dictionary<string, string>
        {
            [AmqpConnectorConfig.Host] = "broker.internal",
            [AmqpConnectorConfig.UseSsl] = "true"
        });

        Assert.Equal(AmqpConnectorConfig.DefaultSslPort, factory.Port);
        Assert.True(factory.Ssl.Enabled);
        Assert.Equal("broker.internal", factory.Ssl.ServerName);
    }

    [Fact]
    public void CreateConnectionFactory_KeepsAnExplicitPortWhenSslIsEnabled()
    {
        var factory = AmqpSourceTask.CreateConnectionFactory(new Dictionary<string, string>
        {
            [AmqpConnectorConfig.Host] = "broker.internal",
            [AmqpConnectorConfig.UseSsl] = "true",
            [AmqpConnectorConfig.Port] = "5999"
        });

        Assert.Equal(5999, factory.Port);
        Assert.True(factory.Ssl.Enabled);
    }

    [Fact]
    public void CreateConnectionFactory_LetsTheUriWinOverTheDiscreteSettings()
    {
        var factory = AmqpSourceTask.CreateConnectionFactory(new Dictionary<string, string>
        {
            [AmqpConnectorConfig.Uri] = "amqp://svc:secret@broker.internal:5680/",
            [AmqpConnectorConfig.Host] = "ignored.example.com",
            [AmqpConnectorConfig.Port] = "1234"
        });

        Assert.Equal("broker.internal", factory.HostName);
        Assert.Equal(5680, factory.Port);
    }
}
