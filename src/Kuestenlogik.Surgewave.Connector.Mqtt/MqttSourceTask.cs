namespace Kuestenlogik.Surgewave.Connector.Mqtt;

using System.Buffers;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect;
using MQTTnet;
using MQTTnet.Protocol;

/// <summary>
/// Task that subscribes to MQTT topics and produces messages to Surgewave.
/// </summary>
public sealed class MqttSourceTask : SourceTask
{
    private IMqttClient? _client;
    private string _surgewaveTopic = "";
    private string _surgewaveTopicPattern = "";
    private string _converter = MqttConnectorConfig.DefaultConverter;
    private long _pollIntervalMs = 100;
    private int _batchMaxRecords = 1000;
    private DateTimeOffset _lastPollTime = DateTimeOffset.MinValue;

    private readonly Dictionary<string, object> _sourcePartition = new();
    private readonly Queue<BufferedMessage> _messageBuffer = new();
    private readonly object _bufferLock = new();

    public override string Version => "1.0.0";

    public override void Start(IDictionary<string, string> config)
    {
        var brokerUrl = config[MqttConnectorConfig.BrokerUrlConfig];
        var topics = config[MqttConnectorConfig.MqttTopicsConfig]
            .Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(t => t.Trim())
            .ToArray();

        _surgewaveTopic = config[MqttConnectorConfig.SurgewaveTopicConfig];
        _surgewaveTopicPattern = GetConfigValue(config, MqttConnectorConfig.SurgewaveTopicPatternConfig, "");
        _converter = GetConfigValue(config, MqttConnectorConfig.MessageConverterConfig, MqttConnectorConfig.DefaultConverter);

        var clientId = GetConfigValue(config, MqttConnectorConfig.ClientIdConfig, "");
        if (string.IsNullOrEmpty(clientId))
            clientId = $"surgewave-mqtt-source-{Guid.NewGuid():N}";

        var qos = (MqttQualityOfServiceLevel)GetConfigInt(config, MqttConnectorConfig.QosConfig, MqttConnectorConfig.DefaultQos);
        var cleanSession = GetConfigBool(config, MqttConnectorConfig.CleanSessionConfig, MqttConnectorConfig.DefaultCleanSession);
        var keepAliveSeconds = GetConfigInt(config, MqttConnectorConfig.KeepAliveSecondsConfig, MqttConnectorConfig.DefaultKeepAliveSeconds);
        var connectionTimeoutSeconds = GetConfigInt(config, MqttConnectorConfig.ConnectionTimeoutSecondsConfig, MqttConnectorConfig.DefaultConnectionTimeoutSeconds);

        _sourcePartition["mqtt.topics"] = string.Join(",", topics);
        _sourcePartition["mqtt.broker"] = brokerUrl;

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

        // Set up message handler before connecting
        _client.ApplicationMessageReceivedAsync += OnMessageReceived;

        // Connect synchronously during Start
        _client.ConnectAsync(options).GetAwaiter().GetResult();

        // Subscribe to topics
        var subscribeOptionsBuilder = new MqttClientSubscribeOptionsBuilder();
        foreach (var topic in topics)
        {
            subscribeOptionsBuilder.WithTopicFilter(topic, qos);
        }

        _client.SubscribeAsync(subscribeOptionsBuilder.Build()).GetAwaiter().GetResult();
    }

    private Task OnMessageReceived(MqttApplicationMessageReceivedEventArgs args)
    {
        // Get payload bytes - convert from ReadOnlySequence<byte> to byte[]
        var payload = args.ApplicationMessage.Payload.ToArray();

        var message = new BufferedMessage
        {
            Topic = args.ApplicationMessage.Topic,
            Payload = payload,
            ReceivedAt = DateTimeOffset.UtcNow,
            Qos = (int)args.ApplicationMessage.QualityOfServiceLevel,
            Retain = args.ApplicationMessage.Retain,
            UserProperties = args.ApplicationMessage.UserProperties?
                .ToDictionary(p => p.Name, p => Encoding.UTF8.GetString(p.ValueBuffer.Span)) ?? new Dictionary<string, string>()
        };

        lock (_bufferLock)
        {
            _messageBuffer.Enqueue(message);
        }

        return Task.CompletedTask;
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

    public override async Task<IReadOnlyList<SourceRecord>> PollAsync(CancellationToken cancellationToken)
    {
        // Handle poll interval
        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastPollTime).TotalMilliseconds;
        if (elapsed < _pollIntervalMs)
        {
            var waitTime = (int)(_pollIntervalMs - elapsed);
            await Task.Delay(waitTime, cancellationToken);
        }

        _lastPollTime = DateTimeOffset.UtcNow;

        var records = new List<SourceRecord>();

        lock (_bufferLock)
        {
            var count = Math.Min(_messageBuffer.Count, _batchMaxRecords);
            for (var i = 0; i < count; i++)
            {
                var msg = _messageBuffer.Dequeue();
                records.Add(CreateSourceRecord(msg));
            }
        }

        return records;
    }

    private SourceRecord CreateSourceRecord(BufferedMessage message)
    {
        var topic = ResolveSurgewaveTopic(message.Topic);
        var value = ConvertPayload(message.Payload);

        var headers = new Dictionary<string, byte[]>
        {
            ["mqtt.topic"] = Encoding.UTF8.GetBytes(message.Topic),
            ["mqtt.qos"] = [(byte)message.Qos],
            ["mqtt.retain"] = [message.Retain ? (byte)1 : (byte)0]
        };

        // Add MQTT 5.0 user properties as headers
        foreach (var (key, val) in message.UserProperties)
        {
            headers[$"mqtt.property.{key}"] = Encoding.UTF8.GetBytes(val);
        }

        return new SourceRecord
        {
            SourcePartition = _sourcePartition,
            SourceOffset = new Dictionary<string, object>
            {
                ["timestamp"] = message.ReceivedAt.ToUnixTimeMilliseconds()
            },
            Topic = topic,
            Key = Encoding.UTF8.GetBytes(message.Topic),
            Value = value,
            Timestamp = message.ReceivedAt,
            Headers = headers
        };
    }

    private string ResolveSurgewaveTopic(string mqttTopic)
    {
        if (string.IsNullOrEmpty(_surgewaveTopicPattern))
            return _surgewaveTopic;

        return _surgewaveTopicPattern.Replace(MqttConnectorConfig.PlaceholderMqttTopic, mqttTopic);
    }

    private byte[] ConvertPayload(byte[] payload)
    {
        return _converter switch
        {
            MqttConnectorConfig.ConverterString => payload, // Already bytes, no conversion needed
            MqttConnectorConfig.ConverterJson => ValidateAndReturnJson(payload),
            _ => payload // bytes - pass through
        };
    }

    private static byte[] ValidateAndReturnJson(byte[] payload)
    {
        // Validate that it's valid JSON, but return original bytes
        try
        {
            using var doc = JsonDocument.Parse(payload);
            return payload;
        }
        catch (JsonException)
        {
            // Wrap invalid JSON in a simple object
            var wrapped = new { raw = Encoding.UTF8.GetString(payload) };
            return JsonSerializer.SerializeToUtf8Bytes(wrapped);
        }
    }

    private static string GetConfigValue(IDictionary<string, string> config, string key, string defaultValue)
        => config.TryGetValue(key, out var value) && !string.IsNullOrEmpty(value) ? value : defaultValue;

    private static int GetConfigInt(IDictionary<string, string> config, string key, int defaultValue)
        => config.TryGetValue(key, out var value) && int.TryParse(value, out var intValue) ? intValue : defaultValue;

    private static bool GetConfigBool(IDictionary<string, string> config, string key, bool defaultValue)
        => config.TryGetValue(key, out var value) && bool.TryParse(value, out var boolValue) ? boolValue : defaultValue;

    private sealed record BufferedMessage
    {
        public required string Topic { get; init; }
        public required byte[] Payload { get; init; }
        public required DateTimeOffset ReceivedAt { get; init; }
        public required int Qos { get; init; }
        public required bool Retain { get; init; }
        public required Dictionary<string, string> UserProperties { get; init; }
    }
}
