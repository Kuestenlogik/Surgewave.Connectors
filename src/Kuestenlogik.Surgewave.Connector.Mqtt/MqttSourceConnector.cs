namespace Kuestenlogik.Surgewave.Connector.Mqtt;

using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

/// <summary>
/// A source connector that subscribes to MQTT topics and produces to Surgewave.
/// </summary>
public sealed class MqttSourceConnector : SourceConnector
{
    private readonly Dictionary<string, string> _config = new();

    public override string Version => "1.0.0";

    public override Type TaskClass => typeof(MqttSourceTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(MqttConnectorConfig.BrokerUrlConfig, ConfigType.String, Importance.High,
            "MQTT broker URL (tcp://host:port or ssl://host:port)")
        .Define(MqttConnectorConfig.MqttTopicsConfig, ConfigType.String, Importance.High,
            "Comma-separated MQTT topics to subscribe to (supports wildcards: +, #)")
        .Define(MqttConnectorConfig.SurgewaveTopicConfig, ConfigType.String, Importance.High,
            "Destination Surgewave topic", EditorHint.Topic)
        .Define(MqttConnectorConfig.ClientIdConfig, ConfigType.String, "", Importance.Medium,
            "MQTT client ID (auto-generated if empty)")
        .Define(MqttConnectorConfig.UsernameConfig, ConfigType.String, "", Importance.Medium,
            "MQTT authentication username")
        .Define(MqttConnectorConfig.PasswordConfig, ConfigType.Password, "", Importance.Medium,
            "MQTT authentication password")
        .Define(MqttConnectorConfig.QosConfig, ConfigType.Int, (long)MqttConnectorConfig.DefaultQos, Importance.Medium,
            "MQTT QoS level (0, 1, or 2)", EditorHint.Select, options: ["0", "1", "2"])
        .Define(MqttConnectorConfig.CleanSessionConfig, ConfigType.Boolean, MqttConnectorConfig.DefaultCleanSession, Importance.Medium,
            "Start with a clean MQTT session")
        .Define(MqttConnectorConfig.KeepAliveSecondsConfig, ConfigType.Int, (long)MqttConnectorConfig.DefaultKeepAliveSeconds, Importance.Low,
            "MQTT keep-alive interval in seconds")
        .Define(MqttConnectorConfig.ConnectionTimeoutSecondsConfig, ConfigType.Int, (long)MqttConnectorConfig.DefaultConnectionTimeoutSeconds, Importance.Low,
            "Connection timeout in seconds")
        .Define(MqttConnectorConfig.SurgewaveTopicPatternConfig, ConfigType.String, "", Importance.Low,
            "Surgewave topic pattern using ${mqtt.topic} placeholder", EditorHint.Topic)
        .Define(MqttConnectorConfig.MessageConverterConfig, ConfigType.String, MqttConnectorConfig.DefaultConverter, Importance.Low,
            "Message converter: bytes, string, or json", EditorHint.Select, options: ["bytes", "string", "json"])
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

        if (!config.TryGetValue(MqttConnectorConfig.MqttTopicsConfig, out var topics) || string.IsNullOrWhiteSpace(topics))
            throw new ArgumentException($"Missing required config: {MqttConnectorConfig.MqttTopicsConfig}");

        if (!config.TryGetValue(MqttConnectorConfig.SurgewaveTopicConfig, out var surgewaveTopic) || string.IsNullOrWhiteSpace(surgewaveTopic))
            throw new ArgumentException($"Missing required config: {MqttConnectorConfig.SurgewaveTopicConfig}");

        // Validate QoS
        var qos = GetConfigInt(config, MqttConnectorConfig.QosConfig, MqttConnectorConfig.DefaultQos);
        if (qos is < 0 or > 2)
            throw new ArgumentException($"Invalid QoS level {qos}. Must be 0, 1, or 2.");

        // Validate converter
        var converter = GetConfigValue(config, MqttConnectorConfig.MessageConverterConfig, MqttConnectorConfig.DefaultConverter);
        if (converter is not (MqttConnectorConfig.ConverterBytes or MqttConnectorConfig.ConverterString or MqttConnectorConfig.ConverterJson))
            throw new ArgumentException($"Invalid converter '{converter}'. Must be '{MqttConnectorConfig.ConverterBytes}', '{MqttConnectorConfig.ConverterString}', or '{MqttConnectorConfig.ConverterJson}'.");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
        _config.Clear();
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // MQTT subscriptions are shared, so a single task handles all topics
        // Multiple tasks would create duplicate messages
        return [new Dictionary<string, string>(_config)];
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;
}
