namespace Kuestenlogik.Surgewave.Connector.Mqtt.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class MqttSinkConnectorTests
{
    [Fact]
    public void MqttSinkConnector_HasCorrectVersion()
    {
        using var connector = new MqttSinkConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void MqttSinkConnector_HasCorrectTaskClass()
    {
        using var connector = new MqttSinkConnector();
        Assert.Equal(typeof(MqttSinkTask), connector.TaskClass);
    }

    [Fact]
    public void MqttSinkConnector_Config_HasRequiredKeys()
    {
        using var connector = new MqttSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "mqtt.broker.url" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "topics" && k.Type == ConfigType.String);
    }

    [Fact]
    public void MqttSinkConnector_Config_HasMqttTopicKeys()
    {
        using var connector = new MqttSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "mqtt.topic");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.topic.pattern");
    }

    [Fact]
    public void MqttSinkConnector_Config_HasOptionalKeys()
    {
        using var connector = new MqttSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "mqtt.client.id");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.username");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.password");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.qos");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.retain");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.message.expiry.seconds");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.clean.session");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.keep.alive.seconds");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.connection.timeout.seconds");
    }

    [Fact]
    public void MqttSinkConnector_Config_HasTlsKeys()
    {
        using var connector = new MqttSinkConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "mqtt.tls.enabled");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.tls.allow.untrusted");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.tls.client.cert.path");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.tls.client.cert.password");
    }

    [Fact]
    public void MqttSinkConnector_Start_ThrowsOnMissingBrokerUrl()
    {
        using var connector = new MqttSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["topics"] = "surgewave-topic",
            ["mqtt.topic"] = "device/commands"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mqtt.broker.url", ex.Message);
    }

    [Fact]
    public void MqttSinkConnector_Start_ThrowsOnMissingSourceTopics()
    {
        using var connector = new MqttSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["mqtt.topic"] = "device/commands"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("topics", ex.Message);
    }

    [Fact]
    public void MqttSinkConnector_Start_ThrowsOnMissingMqttTopic()
    {
        using var connector = new MqttSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["topics"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mqtt.topic", ex.Message);
    }

    [Fact]
    public void MqttSinkConnector_Start_AcceptsMqttTopicPattern()
    {
        using var connector = new MqttSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["topics"] = "surgewave-topic",
            ["mqtt.topic.pattern"] = "device/${surgewave.topic}/commands"
        };

        // Should not throw - pattern is provided instead of static topic
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(3)]
    [InlineData(10)]
    public void MqttSinkConnector_Start_ThrowsOnInvalidQos(int qos)
    {
        using var connector = new MqttSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["topics"] = "surgewave-topic",
            ["mqtt.topic"] = "device/commands",
            ["mqtt.qos"] = qos.ToString()
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("QoS", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void MqttSinkConnector_Start_AcceptsValidQos(int qos)
    {
        using var connector = new MqttSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["topics"] = "surgewave-topic",
            ["mqtt.topic"] = "device/commands",
            ["mqtt.qos"] = qos.ToString()
        };

        // Should not throw
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void MqttSinkConnector_TaskConfigs_ReturnsRequestedNumberOfTasks()
    {
        using var connector = new MqttSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["topics"] = "surgewave-topic",
            ["mqtt.topic"] = "device/commands"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Equal(3, taskConfigs.Count);
    }

    [Fact]
    public void MqttSinkConnector_TaskConfigs_AssignsUniqueTaskIds()
    {
        using var connector = new MqttSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["topics"] = "surgewave-topic",
            ["mqtt.topic"] = "device/commands"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(3);

        Assert.Equal("0", taskConfigs[0]["task.id"]);
        Assert.Equal("1", taskConfigs[1]["task.id"]);
        Assert.Equal("2", taskConfigs[2]["task.id"]);
    }

    [Fact]
    public void MqttSinkConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new MqttSinkConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "ssl://broker.example.com:8883",
            ["topics"] = "surgewave-topic",
            ["mqtt.topic"] = "device/commands",
            ["mqtt.qos"] = "2",
            ["mqtt.retain"] = "true",
            ["mqtt.message.expiry.seconds"] = "3600"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("ssl://broker.example.com:8883", taskConfigs[0]["mqtt.broker.url"]);
        Assert.Equal("surgewave-topic", taskConfigs[0]["topics"]);
        Assert.Equal("device/commands", taskConfigs[0]["mqtt.topic"]);
        Assert.Equal("2", taskConfigs[0]["mqtt.qos"]);
        Assert.Equal("true", taskConfigs[0]["mqtt.retain"]);
        Assert.Equal("3600", taskConfigs[0]["mqtt.message.expiry.seconds"]);
    }

    [Fact]
    public void MqttSinkConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new MqttSinkConnector();
        var config = connector.Config;

        var qosKey = config.Keys.First(k => k.Name == "mqtt.qos");
        Assert.Equal(1L, qosKey.DefaultValue);

        var retainKey = config.Keys.First(k => k.Name == "mqtt.retain");
        Assert.Equal(false, retainKey.DefaultValue);

        var expiryKey = config.Keys.First(k => k.Name == "mqtt.message.expiry.seconds");
        Assert.Equal(0L, expiryKey.DefaultValue);

        var cleanSessionKey = config.Keys.First(k => k.Name == "mqtt.clean.session");
        Assert.Equal(true, cleanSessionKey.DefaultValue);

        var tlsEnabledKey = config.Keys.First(k => k.Name == "mqtt.tls.enabled");
        Assert.Equal(false, tlsEnabledKey.DefaultValue);
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
