namespace Kuestenlogik.Surgewave.Connector.Mqtt;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A sink connector that consumes from Surgewave and publishes to MQTT topics.
/// </summary>
public sealed class MqttSinkConnector : SinkConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(MqttSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(MqttConnectorConfig.BrokerUrlConfig, ConfigType.String, Importance.High,
            "MQTT broker URL (tcp://host:port or ssl://host:port)")
        .Define(MqttConnectorConfig.TopicsConfig, ConfigType.String, Importance.High,
            "Surgewave topics to consume from", EditorHint.Topic)
        .Define(MqttConnectorConfig.MqttTopicConfig, ConfigType.String, "", Importance.High,
            "MQTT topic to publish to (or use mqtt.topic.pattern)")
        .Define(MqttConnectorConfig.MqttTopicPatternConfig, ConfigType.String, "", Importance.Medium,
            "MQTT topic pattern using ${surgewave.topic} or ${key} placeholders")
        .Define(MqttConnectorConfig.ClientIdConfig, ConfigType.String, "", Importance.Medium,
            "MQTT client ID (auto-generated if empty)")
        .Define(MqttConnectorConfig.UsernameConfig, ConfigType.String, "", Importance.Medium,
            "MQTT authentication username")
        .Define(MqttConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.Medium,
            "MQTT authentication password")
        .Define(MqttConnectorConfig.QosConfig, ConfigType.Int, (long)MqttConnectorConfig.DefaultQos, Importance.Medium,
            "MQTT QoS level (0, 1, or 2)", EditorHint.Select, options: ["0", "1", "2"])
        .Define(MqttConnectorConfig.RetainConfig, ConfigType.Boolean, MqttConnectorConfig.DefaultRetain, Importance.Medium,
            "Retain messages on the MQTT broker")
        .Define(MqttConnectorConfig.MessageExpirySecondsConfig, ConfigType.Int, (long)MqttConnectorConfig.DefaultMessageExpirySeconds, Importance.Low,
            "Message expiry interval in seconds (MQTT 5.0, 0 = no expiry)")
        .Define(MqttConnectorConfig.CleanSessionConfig, ConfigType.Boolean, MqttConnectorConfig.DefaultCleanSession, Importance.Medium,
            "Start with a clean MQTT session")
        .Define(MqttConnectorConfig.KeepAliveSecondsConfig, ConfigType.Int, (long)MqttConnectorConfig.DefaultKeepAliveSeconds, Importance.Low,
            "MQTT keep-alive interval in seconds")
        .Define(MqttConnectorConfig.ConnectionTimeoutSecondsConfig, ConfigType.Int, (long)MqttConnectorConfig.DefaultConnectionTimeoutSeconds, Importance.Low,
            "Connection timeout in seconds")
        .Define(MqttConnectorConfig.TlsEnabledConfig, ConfigType.Boolean, MqttConnectorConfig.DefaultTlsEnabled, Importance.Medium,
            "Enable TLS/SSL connection")
        .Define(MqttConnectorConfig.TlsAllowUntrustedConfig, ConfigType.Boolean, MqttConnectorConfig.DefaultTlsAllowUntrusted, Importance.Low,
            "Allow untrusted TLS certificates")
        .Define(MqttConnectorConfig.TlsClientCertPathConfig, ConfigType.String, "", Importance.Low,
            "Path to client certificate file", EditorHint.FilePath)
        .Define(MqttConnectorConfig.TlsClientCertPasswordConfig, ConfigType.Password, "", Importance.Low,
            "Client certificate password");

    public override void Start(IDictionary<string, string> config)
    {
        // Validate required configs
        if (!config.TryGetValue(MqttConnectorConfig.BrokerUrlConfig, out var brokerUrl) || string.IsNullOrWhiteSpace(brokerUrl))
            throw new ArgumentException($"Missing required config: {MqttConnectorConfig.BrokerUrlConfig}");

        if (!config.TryGetValue(MqttConnectorConfig.TopicsConfig, out var topics) || string.IsNullOrWhiteSpace(topics))
            throw new ArgumentException($"Missing required config: {MqttConnectorConfig.TopicsConfig}");

        // Either mqtt.topic or mqtt.topic.pattern must be specified
        var mqttTopic = GetConfigValue(config, MqttConnectorConfig.MqttTopicConfig, "");
        var mqttTopicPattern = GetConfigValue(config, MqttConnectorConfig.MqttTopicPatternConfig, "");

        if (string.IsNullOrWhiteSpace(mqttTopic) && string.IsNullOrWhiteSpace(mqttTopicPattern))
            throw new ArgumentException($"Either {MqttConnectorConfig.MqttTopicConfig} or {MqttConnectorConfig.MqttTopicPatternConfig} must be specified");

        // Validate QoS
        var qos = GetConfigInt(config, MqttConnectorConfig.QosConfig, MqttConnectorConfig.DefaultQos);
        if (qos is < 0 or > 2)
            throw new ArgumentException($"Invalid QoS level {qos}. Must be 0, 1, or 2.");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Each task gets a copy of the full config
        // Tasks can be scaled to handle different partitions
        var configs = new List<IDictionary<string, string>>();

        for (var i = 0; i < maxTasks; i++)
        {
            configs.Add(new Dictionary<string, string>(_config)
            {
                ["task.id"] = i.ToString()
            });
        }

        return configs;
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;
}
