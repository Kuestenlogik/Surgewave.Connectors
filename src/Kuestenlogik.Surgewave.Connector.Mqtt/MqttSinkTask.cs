namespace Kuestenlogik.Surgewave.Connector.Mqtt;

using System.Text;
using Kuestenlogik.Surgewave.Connect;
using MQTTnet;
using MQTTnet.Protocol;

/// <summary>
/// Task that consumes from Surgewave and publishes messages to MQTT.
/// </summary>
public sealed class MqttSinkTask : SinkTask
{
    private IMqttClient? _client;
    private string _mqttTopic = "";
    private string _mqttTopicPattern = "";
    private MqttQualityOfServiceLevel _qos = MqttQualityOfServiceLevel.AtLeastOnce;
    private bool _retain;
    private int _messageExpirySeconds;

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var brokerUrl = config[MqttConnectorConfig.BrokerUrlConfig];

        _mqttTopic = GetConfigValue(config, MqttConnectorConfig.MqttTopicConfig, "");
        _mqttTopicPattern = GetConfigValue(config, MqttConnectorConfig.MqttTopicPatternConfig, "");
        _qos = (MqttQualityOfServiceLevel)GetConfigInt(config, MqttConnectorConfig.QosConfig, MqttConnectorConfig.DefaultQos);
        _retain = GetConfigBool(config, MqttConnectorConfig.RetainConfig, MqttConnectorConfig.DefaultRetain);
        _messageExpirySeconds = GetConfigInt(config, MqttConnectorConfig.MessageExpirySecondsConfig, MqttConnectorConfig.DefaultMessageExpirySeconds);

        var clientId = GetConfigValue(config, MqttConnectorConfig.ClientIdConfig, "");
        if (string.IsNullOrEmpty(clientId))
        {
            var taskId = GetConfigValue(config, "task.id", "0");
            clientId = $"surgewave-mqtt-sink-{taskId}-{Guid.NewGuid():N}";
        }

        var cleanSession = GetConfigBool(config, MqttConnectorConfig.CleanSessionConfig, MqttConnectorConfig.DefaultCleanSession);
        var keepAliveSeconds = GetConfigInt(config, MqttConnectorConfig.KeepAliveSecondsConfig, MqttConnectorConfig.DefaultKeepAliveSeconds);
        var connectionTimeoutSeconds = GetConfigInt(config, MqttConnectorConfig.ConnectionTimeoutSecondsConfig, MqttConnectorConfig.DefaultConnectionTimeoutSeconds);

        // Parse broker URL
        var uri = new Uri(brokerUrl);
        var useTls = uri.Scheme.Equals("ssl", StringComparison.OrdinalIgnoreCase) ||
                     uri.Scheme.Equals("tls", StringComparison.OrdinalIgnoreCase) ||
                     GetConfigBool(config, MqttConnectorConfig.TlsEnabledConfig, MqttConnectorConfig.DefaultTlsEnabled);

        var optionsBuilder = new MqttClientOptionsBuilder()
            .WithClientId(clientId)
            .WithTcpServer(uri.Host, uri.Port > 0 ? uri.Port : (useTls ? 8883 : 1883))
            .WithCleanSession(cleanSession)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(keepAliveSeconds))
            .WithTimeout(TimeSpan.FromSeconds(connectionTimeoutSeconds));

        // Configure authentication
        var username = GetConfigValue(config, MqttConnectorConfig.UsernameConfig, "");
        var password = GetConfigValue(config, MqttConnectorConfig.PasswordConfig, "");
        if (!string.IsNullOrEmpty(username))
        {
            optionsBuilder.WithCredentials(username, password);
        }

        // Configure TLS
        if (useTls)
        {
            var allowUntrusted = GetConfigBool(config, MqttConnectorConfig.TlsAllowUntrustedConfig, MqttConnectorConfig.DefaultTlsAllowUntrusted);
            var tlsOptionsBuilder = new MqttClientTlsOptionsBuilder()
                .UseTls();

            if (allowUntrusted)
            {
                tlsOptionsBuilder.WithAllowUntrustedCertificates();
            }

            optionsBuilder.WithTlsOptions(tlsOptionsBuilder.Build());
        }

        var options = optionsBuilder.Build();

        // Create and connect client
        var factory = new MqttClientFactory();
        _client = factory.CreateMqttClient();

        // Connect synchronously during Start
        _client.ConnectAsync(options).GetAwaiter().GetResult();
    }

    public override void Stop()
    {
        if (_client?.IsConnected == true)
        {
            _client.DisconnectAsync().GetAwaiter().GetResult();
        }

        _client?.Dispose();
        _client = null;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            if (_client?.IsConnected == true)
            {
                try
                {
                    _client.DisconnectAsync().GetAwaiter().GetResult();
                }
                catch
                {
                    // Ignore disposal errors
                }
            }

            _client?.Dispose();
        }

        base.Dispose(disposing);
    }

    public override async Task PutAsync(IReadOnlyList<SinkRecord> records, CancellationToken cancellationToken)
    {
        if (_client == null || !_client.IsConnected)
            throw new InvalidOperationException("MQTT client is not connected");

        foreach (var record in records)
        {
            var topic = ResolveMqttTopic(record);

            var messageBuilder = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(record.Value)
                .WithQualityOfServiceLevel(_qos)
                .WithRetainFlag(_retain);

            // Add message expiry for MQTT 5.0
            if (_messageExpirySeconds > 0)
            {
                messageBuilder.WithMessageExpiryInterval((uint)_messageExpirySeconds);
            }

            // Add user properties from record headers (MQTT 5.0)
            if (record.Headers != null)
            {
                foreach (var (key, value) in record.Headers)
                {
                    // Only forward headers that don't start with internal prefixes
                    if (!key.StartsWith("mqtt.", StringComparison.OrdinalIgnoreCase) &&
                        !key.StartsWith("surgewave.", StringComparison.OrdinalIgnoreCase))
                    {
                        messageBuilder.WithUserProperty(key, (ReadOnlyMemory<byte>)value);
                    }
                }
            }

            var message = messageBuilder.Build();

            await _client.PublishAsync(message, cancellationToken);
        }
    }

    private string ResolveMqttTopic(SinkRecord record)
    {
        if (string.IsNullOrEmpty(_mqttTopicPattern))
            return _mqttTopic;

        var topic = _mqttTopicPattern;

        // Replace placeholders
        topic = topic.Replace(MqttConnectorConfig.PlaceholderSurgewaveTopic, record.Topic);

        if (record.Key != null && topic.Contains(MqttConnectorConfig.PlaceholderKey))
        {
            var keyString = Encoding.UTF8.GetString(record.Key);
            topic = topic.Replace(MqttConnectorConfig.PlaceholderKey, keyString);
        }

        return topic;
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;

    private static bool GetConfigBool(IDictionary<string, string> config, string key, bool defaultValue)
        => config.TryGetValue(key, out var value) && bool.TryParse(value, out var boolValue) ? boolValue : defaultValue;
}
