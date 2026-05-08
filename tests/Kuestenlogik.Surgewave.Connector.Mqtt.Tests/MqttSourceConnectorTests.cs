namespace Kuestenlogik.Surgewave.Connector.Mqtt.Tests;

using Kuestenlogik.Surgewave.Plugins.Configuration;
using Xunit;
using Kuestenlogik.Surgewave.Connect;

public sealed class MqttSourceConnectorTests
{
    [Fact]
    public void MqttSourceConnector_HasCorrectVersion()
    {
        using var connector = new MqttSourceConnector();
        Assert.Equal("1.0.0", connector.Version);
    }

    [Fact]
    public void MqttSourceConnector_HasCorrectTaskClass()
    {
        using var connector = new MqttSourceConnector();
        Assert.Equal(typeof(MqttSourceTask), connector.TaskClass);
    }

    [Fact]
    public void MqttSourceConnector_Config_HasRequiredKeys()
    {
        using var connector = new MqttSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "mqtt.broker.url" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "mqtt.topics" && k.Type == ConfigType.String);
        Assert.Contains(config.Keys, k => k.Name == "surgewave.topic" && k.Type == ConfigType.String);
    }

    [Fact]
    public void MqttSourceConnector_Config_HasOptionalKeys()
    {
        using var connector = new MqttSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "mqtt.client.id");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.username");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.password");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.qos");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.clean.session");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.keep.alive.seconds");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.connection.timeout.seconds");
        Assert.Contains(config.Keys, k => k.Name == "surgewave.topic.pattern");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.message.converter");
    }

    [Fact]
    public void MqttSourceConnector_Config_HasTlsKeys()
    {
        using var connector = new MqttSourceConnector();
        var config = connector.Config;

        Assert.Contains(config.Keys, k => k.Name == "mqtt.tls.enabled");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.tls.allow.untrusted");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.tls.client.cert.path");
        Assert.Contains(config.Keys, k => k.Name == "mqtt.tls.client.cert.password");
    }

    [Fact]
    public void MqttSourceConnector_Start_ThrowsOnMissingBrokerUrl()
    {
        using var connector = new MqttSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.topics"] = "sensor/temperature",
            ["surgewave.topic"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mqtt.broker.url", ex.Message);
    }

    [Fact]
    public void MqttSourceConnector_Start_ThrowsOnMissingTopics()
    {
        using var connector = new MqttSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["surgewave.topic"] = "surgewave-topic"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("mqtt.topics", ex.Message);
    }

    [Fact]
    public void MqttSourceConnector_Start_ThrowsOnMissingSurgewaveTopic()
    {
        using var connector = new MqttSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["mqtt.topics"] = "sensor/temperature"
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("surgewave.topic", ex.Message);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(3)]
    [InlineData(10)]
    public void MqttSourceConnector_Start_ThrowsOnInvalidQos(int qos)
    {
        using var connector = new MqttSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["mqtt.topics"] = "sensor/temperature",
            ["surgewave.topic"] = "surgewave-topic",
            ["mqtt.qos"] = qos.ToString()
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("QoS", ex.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void MqttSourceConnector_Start_AcceptsValidQos(int qos)
    {
        using var connector = new MqttSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["mqtt.topics"] = "sensor/temperature",
            ["surgewave.topic"] = "surgewave-topic",
            ["mqtt.qos"] = qos.ToString()
        };

        // Should not throw for validation (will fail on actual connection, but validation passes)
        connector.Start(config);
        connector.Stop();
    }

    [Theory]
    [InlineData("invalid")]
    [InlineData("xml")]
    [InlineData("protobuf")]
    public void MqttSourceConnector_Start_ThrowsOnInvalidConverter(string converter)
    {
        using var connector = new MqttSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["mqtt.topics"] = "sensor/temperature",
            ["surgewave.topic"] = "surgewave-topic",
            ["mqtt.message.converter"] = converter
        };

        var ex = Assert.Throws<ArgumentException>(() => connector.Start(config));
        Assert.Contains("converter", ex.Message);
    }

    [Theory]
    [InlineData("bytes")]
    [InlineData("string")]
    [InlineData("json")]
    public void MqttSourceConnector_Start_AcceptsValidConverter(string converter)
    {
        using var connector = new MqttSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["mqtt.topics"] = "sensor/temperature",
            ["surgewave.topic"] = "surgewave-topic",
            ["mqtt.message.converter"] = converter
        };

        // Should not throw for validation
        connector.Start(config);
        connector.Stop();
    }

    [Fact]
    public void MqttSourceConnector_TaskConfigs_ReturnsSingleTask()
    {
        using var connector = new MqttSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["mqtt.topics"] = "sensor/+/temperature,device/#",
            ["surgewave.topic"] = "surgewave-topic"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(5);

        // MQTT subscriptions are shared, so only one task
        Assert.Single(taskConfigs);
    }

    [Fact]
    public void MqttSourceConnector_TaskConfigs_PreservesAllConfig()
    {
        using var connector = new MqttSourceConnector();
        connector.Initialize(CreateContext());

        var config = new Dictionary<string, string>
        {
            ["mqtt.broker.url"] = "tcp://localhost:1883",
            ["mqtt.topics"] = "sensor/temperature",
            ["surgewave.topic"] = "surgewave-topic",
            ["mqtt.qos"] = "2",
            ["mqtt.clean.session"] = "false"
        };

        connector.Start(config);
        var taskConfigs = connector.TaskConfigs(1);

        Assert.Equal("tcp://localhost:1883", taskConfigs[0]["mqtt.broker.url"]);
        Assert.Equal("sensor/temperature", taskConfigs[0]["mqtt.topics"]);
        Assert.Equal("surgewave-topic", taskConfigs[0]["surgewave.topic"]);
        Assert.Equal("2", taskConfigs[0]["mqtt.qos"]);
        Assert.Equal("false", taskConfigs[0]["mqtt.clean.session"]);
    }

    [Fact]
    public void MqttSourceConnector_Config_HasCorrectDefaultValues()
    {
        using var connector = new MqttSourceConnector();
        var config = connector.Config;

        var qosKey = config.Keys.First(k => k.Name == "mqtt.qos");
        Assert.Equal(1L, qosKey.DefaultValue);

        var cleanSessionKey = config.Keys.First(k => k.Name == "mqtt.clean.session");
        Assert.Equal(true, cleanSessionKey.DefaultValue);

        var keepAliveKey = config.Keys.First(k => k.Name == "mqtt.keep.alive.seconds");
        Assert.Equal(60L, keepAliveKey.DefaultValue);

        var converterKey = config.Keys.First(k => k.Name == "mqtt.message.converter");
        Assert.Equal("bytes", converterKey.DefaultValue);

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
