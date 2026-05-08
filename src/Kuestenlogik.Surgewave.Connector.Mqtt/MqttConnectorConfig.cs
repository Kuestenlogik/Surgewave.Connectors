namespace Kuestenlogik.Surgewave.Connector.Mqtt;

/// <summary>
/// Shared configuration constants for MQTT connectors.
/// </summary>
internal static class MqttConnectorConfig
{
    // Connection configs
    public const string BrokerUrlConfig = "mqtt.broker.url";
    public const string ClientIdConfig = "mqtt.client.id";
    public const string UsernameConfig = "mqtt.username";
    public const string PasswordConfig = "mqtt.password";
    public const string CleanSessionConfig = "mqtt.clean.session";
    public const string KeepAliveSecondsConfig = "mqtt.keep.alive.seconds";
    public const string ConnectionTimeoutSecondsConfig = "mqtt.connection.timeout.seconds";

    // TLS configs
    public const string TlsEnabledConfig = "mqtt.tls.enabled";
    public const string TlsAllowUntrustedConfig = "mqtt.tls.allow.untrusted";
    public const string TlsClientCertPathConfig = "mqtt.tls.client.cert.path";
    public const string TlsClientCertPasswordConfig = "mqtt.tls.client.cert.password";

    // Source-specific configs
    public const string MqttTopicsConfig = "mqtt.topics";
    public const string SurgewaveTopicConfig = "surgewave.topic";
    public const string SurgewaveTopicPatternConfig = "surgewave.topic.pattern";
    public const string MessageConverterConfig = "mqtt.message.converter";

    // Sink-specific configs
    public const string MqttTopicConfig = "mqtt.topic";
    public const string MqttTopicPatternConfig = "mqtt.topic.pattern";
    public const string RetainConfig = "mqtt.retain";
    public const string MessageExpirySecondsConfig = "mqtt.message.expiry.seconds";

    // Common configs
    public const string QosConfig = "mqtt.qos";
    public const string TopicsConfig = "topics";

    // Message converters
    public const string ConverterBytes = "bytes";
    public const string ConverterString = "string";
    public const string ConverterJson = "json";

    // Default values
    public const bool DefaultCleanSession = true;
    public const int DefaultKeepAliveSeconds = 60;
    public const int DefaultConnectionTimeoutSeconds = 30;
    public const int DefaultQos = 1;
    public const bool DefaultTlsEnabled = false;
    public const bool DefaultTlsAllowUntrusted = false;
    public const bool DefaultRetain = false;
    public const int DefaultMessageExpirySeconds = 0;
    public const int DefaultMaxPendingMessages = 10000;
    public const string DefaultConverter = ConverterBytes;

    // Topic pattern placeholders
    public const string PlaceholderMqttTopic = "${mqtt.topic}";
    public const string PlaceholderSurgewaveTopic = "${surgewave.topic}";
    public const string PlaceholderKey = "${key}";
}
