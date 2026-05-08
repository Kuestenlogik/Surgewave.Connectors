using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.SocketStream;

/// <summary>
/// A source connector that connects to a TCP or Unix socket server and consumes a continuous message stream.
/// Unlike the TCP connector which operates in server mode, this connector operates in client mode,
/// connecting to an existing server and consuming data as it arrives.
/// </summary>
[ConnectorMetadata(
    Name = "Socket Stream Source",
    Description = "Connects to a TCP or Unix socket server and consumes a continuous message stream with configurable framing.",
    Author = "KL Surgewave",
    Tags = "socket,tcp,unix,stream,client,network",
    Icon = "NetworkWired")]
public sealed class SocketStreamSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SocketStreamSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Connection settings
        .Define(SocketStreamConnectorConfig.SocketType, ConfigType.String, SocketStreamConnectorConfig.DefaultSocketType, Importance.High,
            "Socket type: 'tcp' for TCP sockets or 'unix' for Unix domain sockets")
        .Define(SocketStreamConnectorConfig.Host, ConfigType.String, SocketStreamConnectorConfig.DefaultHost, Importance.High,
            "Host to connect to (for TCP sockets)")
        .Define(SocketStreamConnectorConfig.Port, ConfigType.Int, SocketStreamConnectorConfig.DefaultPort, Importance.High,
            "Port to connect to (for TCP sockets)")
        .Define(SocketStreamConnectorConfig.UnixSocketPath, ConfigType.String, "", Importance.High,
            "Path to Unix domain socket (for Unix sockets)", EditorHint.FilePath)
        .Define(SocketStreamConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Topic to produce messages to", EditorHint.Topic)

        // Connection management
        .Define(SocketStreamConnectorConfig.ConnectTimeoutMs, ConfigType.Int, SocketStreamConnectorConfig.DefaultConnectTimeoutMs, Importance.Medium,
            "Connection timeout in milliseconds")
        .Define(SocketStreamConnectorConfig.ReadTimeoutMs, ConfigType.Int, SocketStreamConnectorConfig.DefaultReadTimeoutMs, Importance.Low,
            "Read timeout in milliseconds (0 = no timeout)")
        .Define(SocketStreamConnectorConfig.ReconnectEnabled, ConfigType.Boolean, SocketStreamConnectorConfig.DefaultReconnectEnabled, Importance.Medium,
            "Enable automatic reconnection on disconnect")
        .Define(SocketStreamConnectorConfig.ReconnectDelayMs, ConfigType.Int, SocketStreamConnectorConfig.DefaultReconnectDelayMs, Importance.Low,
            "Initial delay before reconnecting in milliseconds")
        .Define(SocketStreamConnectorConfig.ReconnectMaxDelayMs, ConfigType.Int, SocketStreamConnectorConfig.DefaultReconnectMaxDelayMs, Importance.Low,
            "Maximum delay between reconnection attempts in milliseconds")
        .Define(SocketStreamConnectorConfig.ReconnectMaxAttempts, ConfigType.Int, SocketStreamConnectorConfig.DefaultReconnectMaxAttempts, Importance.Low,
            "Maximum number of reconnection attempts (-1 = unlimited)")

        // Framing
        .Define(SocketStreamConnectorConfig.Framing, ConfigType.String, SocketStreamConnectorConfig.DefaultFraming, Importance.Medium,
            "Message framing mode: 'line', 'delimiter', 'length-prefix', or 'raw'", EditorHint.Select, options: ["newline", "length-prefixed", "raw"])
        .Define(SocketStreamConnectorConfig.Delimiter, ConfigType.String, SocketStreamConnectorConfig.DefaultDelimiter, Importance.Low,
            "Delimiter bytes for 'delimiter' framing mode (supports \\n, \\r, \\t escapes)")
        .Define(SocketStreamConnectorConfig.LengthPrefixBytes, ConfigType.Int, SocketStreamConnectorConfig.DefaultLengthPrefixBytes, Importance.Low,
            "Number of bytes for length prefix (1, 2, or 4) in 'length-prefix' mode")
        .Define(SocketStreamConnectorConfig.LengthPrefixBigEndian, ConfigType.Boolean, SocketStreamConnectorConfig.DefaultLengthPrefixBigEndian, Importance.Low,
            "Use big-endian byte order for length prefix")
        .Define(SocketStreamConnectorConfig.MaxMessageSize, ConfigType.Int, SocketStreamConnectorConfig.DefaultMaxMessageSize, Importance.Low,
            "Maximum message size in bytes")

        // Buffer settings
        .Define(SocketStreamConnectorConfig.ReceiveBufferSize, ConfigType.Int, SocketStreamConnectorConfig.DefaultReceiveBufferSize, Importance.Low,
            "Socket receive buffer size in bytes")
        .Define(SocketStreamConnectorConfig.SendBufferSize, ConfigType.Int, SocketStreamConnectorConfig.DefaultSendBufferSize, Importance.Low,
            "Socket send buffer size in bytes")

        // TLS settings
        .Define(SocketStreamConnectorConfig.TlsEnabled, ConfigType.Boolean, false, Importance.Medium,
            "Enable TLS encryption (TCP only)")
        .Define(SocketStreamConnectorConfig.TlsServerName, ConfigType.String, "", Importance.Low,
            "Server name for TLS certificate validation (defaults to host)")
        .Define(SocketStreamConnectorConfig.TlsValidateCertificate, ConfigType.Boolean, true, Importance.Low,
            "Validate server TLS certificate")
        .Define(SocketStreamConnectorConfig.TlsClientCertPath, ConfigType.String, "", Importance.Low,
            "Path to client certificate for mutual TLS", EditorHint.FilePath)
        .Define(SocketStreamConnectorConfig.TlsClientCertPassword, ConfigType.Password, "", Importance.Low,
            "Password for client certificate");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        // Validate required settings
        if (!config.TryGetValue(SocketStreamConnectorConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException($"Missing required config: {SocketStreamConnectorConfig.Topic}");
        }

        var socketType = config.TryGetValue(SocketStreamConnectorConfig.SocketType, out var st)
            ? st : SocketStreamConnectorConfig.DefaultSocketType;

        if (socketType == SocketStreamConnectorConfig.SocketTypeUnix)
        {
            if (!config.TryGetValue(SocketStreamConnectorConfig.UnixSocketPath, out var path) || string.IsNullOrEmpty(path))
            {
                throw new ArgumentException($"Unix socket type requires {SocketStreamConnectorConfig.UnixSocketPath}");
            }
        }
        else if (socketType == SocketStreamConnectorConfig.SocketTypeTcp)
        {
            var port = config.TryGetValue(SocketStreamConnectorConfig.Port, out var portStr) && int.TryParse(portStr, out var p)
                ? p : SocketStreamConnectorConfig.DefaultPort;
            if (port < 1 || port > 65535)
            {
                throw new ArgumentException($"Invalid port: {port}. Must be between 1 and 65535.");
            }
        }
        else
        {
            throw new ArgumentException($"Invalid socket type: {socketType}. Must be 'tcp' or 'unix'.");
        }

        // Validate framing
        var framing = config.TryGetValue(SocketStreamConnectorConfig.Framing, out var f)
            ? f : SocketStreamConnectorConfig.DefaultFraming;
        if (framing != SocketStreamConnectorConfig.FramingLine &&
            framing != SocketStreamConnectorConfig.FramingDelimiter &&
            framing != SocketStreamConnectorConfig.FramingLengthPrefix &&
            framing != SocketStreamConnectorConfig.FramingRaw)
        {
            throw new ArgumentException($"Invalid framing mode: {framing}. Must be 'line', 'delimiter', 'length-prefix', or 'raw'.");
        }

        if (framing == SocketStreamConnectorConfig.FramingLengthPrefix)
        {
            var prefixBytes = config.TryGetValue(SocketStreamConnectorConfig.LengthPrefixBytes, out var pb) && int.TryParse(pb, out var bytes)
                ? bytes : SocketStreamConnectorConfig.DefaultLengthPrefixBytes;
            if (prefixBytes != 1 && prefixBytes != 2 && prefixBytes != 4)
            {
                throw new ArgumentException($"Invalid length prefix bytes: {prefixBytes}. Must be 1, 2, or 4.");
            }
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - one socket connection
        return [new Dictionary<string, string>(_config)];
    }
}
