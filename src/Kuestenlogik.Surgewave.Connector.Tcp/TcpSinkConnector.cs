using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Connect;

namespace Kuestenlogik.Surgewave.Connector.Tcp;

/// <summary>
/// A sink connector that sends records to a TCP endpoint.
/// Supports TLS encryption and multiple framing modes (line, length-prefix, delimiter).
/// </summary>
public sealed class TcpSinkConnector : SinkConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(TcpSinkTask);

    public override ConfigDef Config => new ConfigDef()
        .Define(TcpConnectorConfig.Topics, ConfigType.String, Importance.High,
            "Topics to consume records from (comma-separated)", EditorHint.Topic)
        .Define(TcpConnectorConfig.Host, ConfigType.String, Importance.High,
            "TCP host to connect to")
        .Define(TcpConnectorConfig.Port, ConfigType.Int, Importance.High,
            "TCP port to connect to")
        .Define(TcpConnectorConfig.Framing, ConfigType.String,
            TcpConnectorConfig.DefaultFraming, Importance.Medium,
            "Message framing: 'line', 'length-prefix', or 'delimiter'")
        .Define(TcpConnectorConfig.Delimiter, ConfigType.String,
            TcpConnectorConfig.DefaultDelimiter, Importance.Low,
            "Delimiter for 'delimiter' framing mode")
        .Define(TcpConnectorConfig.LengthPrefixBytes, ConfigType.Int,
            TcpConnectorConfig.DefaultLengthPrefixBytes, Importance.Low,
            "Number of bytes for length prefix (1, 2, or 4)")
        .Define(TcpConnectorConfig.LengthPrefixBigEndian, ConfigType.Boolean,
            TcpConnectorConfig.DefaultLengthPrefixBigEndian, Importance.Low,
            "Use big-endian byte order for length prefix")
        .Define(TcpConnectorConfig.MaxMessageSize, ConfigType.Int,
            TcpConnectorConfig.DefaultMaxMessageSize, Importance.Low,
            "Maximum message size in bytes")
        .Define(TcpConnectorConfig.UseTls, ConfigType.Boolean,
            TcpConnectorConfig.DefaultUseTls, Importance.Medium,
            "Enable TLS encryption")
        .Define(TcpConnectorConfig.TlsValidateCertificate, ConfigType.Boolean,
            TcpConnectorConfig.DefaultTlsValidateCertificate, Importance.Low,
            "Validate server TLS certificate")
        .Define(TcpConnectorConfig.Reconnect, ConfigType.Boolean,
            TcpConnectorConfig.DefaultReconnect, Importance.Medium,
            "Enable automatic reconnection")
        .Define(TcpConnectorConfig.ReconnectDelayMs, ConfigType.Int,
            TcpConnectorConfig.DefaultReconnectDelayMs, Importance.Low,
            "Initial delay between reconnection attempts (ms)")
        .Define(TcpConnectorConfig.ReconnectMaxDelayMs, ConfigType.Int,
            TcpConnectorConfig.DefaultReconnectMaxDelayMs, Importance.Low,
            "Maximum delay between reconnection attempts (ms)")
        .Define(TcpConnectorConfig.SendBufferSize, ConfigType.Int,
            TcpConnectorConfig.DefaultSendBufferSize, Importance.Low,
            "TCP send buffer size")
        .Define(TcpConnectorConfig.ConnectionTimeout, ConfigType.Int,
            TcpConnectorConfig.DefaultConnectionTimeout, Importance.Low,
            "Connection timeout in milliseconds");

    private readonly Dictionary<string, string> _config = new();

    public override void Start(IDictionary<string, string> config)
    {
        if (!config.ContainsKey(TcpConnectorConfig.Topics))
            throw new ArgumentException($"Missing required config: {TcpConnectorConfig.Topics}");
        if (!config.ContainsKey(TcpConnectorConfig.Host))
            throw new ArgumentException($"Missing required config: {TcpConnectorConfig.Host}");
        if (!config.ContainsKey(TcpConnectorConfig.Port))
            throw new ArgumentException($"Missing required config: {TcpConnectorConfig.Port}");

        foreach (var kvp in config)
            _config[kvp.Key] = kvp.Value;
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // TCP client only supports a single task per endpoint
        return [new Dictionary<string, string>(_config)];
    }
}
