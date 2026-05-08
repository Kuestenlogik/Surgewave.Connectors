using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins.Configuration;

namespace Kuestenlogik.Surgewave.Connector.SocketServer;

/// <summary>
/// A source connector that listens on TCP, UDP, or Unix sockets and receives messages from multiple clients.
/// Supports concurrent client connections for TCP/Unix and datagram reception for UDP.
/// </summary>
[ConnectorMetadata(
    Name = "Socket Server Source",
    Description = "Listens on TCP, UDP, or Unix sockets and receives messages from multiple clients with configurable framing.",
    Author = "KL Surgewave",
    Tags = "socket,tcp,udp,unix,server,listen,network",
    Icon = "Server")]
public sealed class SocketServerSourceConnector : SourceConnector
{
    public override string Version => "1.0.0";
    public override Type TaskClass => typeof(SocketServerSourceTask);

    private IDictionary<string, string> _config = new Dictionary<string, string>();

    public override ConfigDef Config => new ConfigDef()
        // Server settings
        .Define(SocketServerConnectorConfig.Protocol, ConfigType.String, SocketServerConnectorConfig.DefaultProtocol, Importance.High,
            "Socket protocol: 'tcp', 'udp', or 'unix'", EditorHint.Select, options: ["tcp", "udp", "unix"])
        .Define(SocketServerConnectorConfig.ListenAddress, ConfigType.String, SocketServerConnectorConfig.DefaultListenAddress, Importance.Medium,
            "Address to listen on (for TCP/UDP)")
        .Define(SocketServerConnectorConfig.ListenPort, ConfigType.Int, SocketServerConnectorConfig.DefaultListenPort, Importance.High,
            "Port to listen on (for TCP/UDP)")
        .Define(SocketServerConnectorConfig.UnixSocketPath, ConfigType.String, "", Importance.High,
            "Path to Unix domain socket (for Unix protocol)", EditorHint.FilePath)
        .Define(SocketServerConnectorConfig.Topic, ConfigType.String, Importance.High,
            "Topic to produce messages to", EditorHint.Topic)

        // Connection management
        .Define(SocketServerConnectorConfig.MaxConnections, ConfigType.Int, SocketServerConnectorConfig.DefaultMaxConnections, Importance.Medium,
            "Maximum number of concurrent client connections (TCP/Unix only)")
        .Define(SocketServerConnectorConfig.ConnectionTimeoutMs, ConfigType.Int, SocketServerConnectorConfig.DefaultConnectionTimeoutMs, Importance.Low,
            "Connection timeout for new clients in milliseconds")
        .Define(SocketServerConnectorConfig.IdleTimeoutMs, ConfigType.Int, SocketServerConnectorConfig.DefaultIdleTimeoutMs, Importance.Low,
            "Idle timeout for client connections in milliseconds (0 = no timeout)")

        // Framing (TCP/Unix only)
        .Define(SocketServerConnectorConfig.Framing, ConfigType.String, SocketServerConnectorConfig.DefaultFraming, Importance.Medium,
            "Message framing mode: 'line', 'delimiter', 'length-prefix', or 'raw' (TCP/Unix only)", EditorHint.Select, options: ["newline", "length-prefixed", "raw"])
        .Define(SocketServerConnectorConfig.Delimiter, ConfigType.String, SocketServerConnectorConfig.DefaultDelimiter, Importance.Low,
            "Delimiter bytes for 'delimiter' framing mode (supports \\n, \\r, \\t escapes)")
        .Define(SocketServerConnectorConfig.LengthPrefixBytes, ConfigType.Int, SocketServerConnectorConfig.DefaultLengthPrefixBytes, Importance.Low,
            "Number of bytes for length prefix (1, 2, or 4) in 'length-prefix' mode")
        .Define(SocketServerConnectorConfig.LengthPrefixBigEndian, ConfigType.Boolean, SocketServerConnectorConfig.DefaultLengthPrefixBigEndian, Importance.Low,
            "Use big-endian byte order for length prefix")
        .Define(SocketServerConnectorConfig.MaxMessageSize, ConfigType.Int, SocketServerConnectorConfig.DefaultMaxMessageSize, Importance.Low,
            "Maximum message size in bytes")

        // Buffer settings
        .Define(SocketServerConnectorConfig.ReceiveBufferSize, ConfigType.Int, SocketServerConnectorConfig.DefaultReceiveBufferSize, Importance.Low,
            "Socket receive buffer size in bytes")
        .Define(SocketServerConnectorConfig.SendBufferSize, ConfigType.Int, SocketServerConnectorConfig.DefaultSendBufferSize, Importance.Low,
            "Socket send buffer size in bytes")

        // TLS settings (TCP only)
        .Define(SocketServerConnectorConfig.TlsEnabled, ConfigType.Boolean, false, Importance.Medium,
            "Enable TLS encryption (TCP only)")
        .Define(SocketServerConnectorConfig.TlsCertificatePath, ConfigType.String, "", Importance.Medium,
            "Path to TLS certificate file (PFX/PKCS12)", EditorHint.FilePath)
        .Define(SocketServerConnectorConfig.TlsCertificatePassword, ConfigType.Password, "", Importance.Medium,
            "Password for TLS certificate")
        .Define(SocketServerConnectorConfig.TlsRequireClientCert, ConfigType.Boolean, false, Importance.Low,
            "Require client certificate for mutual TLS")

        // UDP settings
        .Define(SocketServerConnectorConfig.UdpMulticastEnabled, ConfigType.Boolean, false, Importance.Low,
            "Enable multicast reception (UDP only)")
        .Define(SocketServerConnectorConfig.UdpMulticastGroup, ConfigType.String, "", Importance.Low,
            "Multicast group address to join (UDP only)")
        .Define(SocketServerConnectorConfig.UdpIncludeSourceInfo, ConfigType.Boolean, SocketServerConnectorConfig.DefaultUdpIncludeSourceInfo, Importance.Low,
            "Include source IP/port in message headers (UDP only)")

        // Metadata
        .Define(SocketServerConnectorConfig.IncludeClientInfo, ConfigType.Boolean, SocketServerConnectorConfig.DefaultIncludeClientInfo, Importance.Low,
            "Include client connection info in message headers");

    public override void Start(IDictionary<string, string> config)
    {
        _config = new Dictionary<string, string>(config);

        // Validate required settings
        if (!config.TryGetValue(SocketServerConnectorConfig.Topic, out var topic) || string.IsNullOrEmpty(topic))
        {
            throw new ArgumentException($"Missing required config: {SocketServerConnectorConfig.Topic}");
        }

        var protocol = config.TryGetValue(SocketServerConnectorConfig.Protocol, out var p)
            ? p : SocketServerConnectorConfig.DefaultProtocol;

        if (protocol == SocketServerConnectorConfig.ProtocolUnix)
        {
            if (!config.TryGetValue(SocketServerConnectorConfig.UnixSocketPath, out var path) || string.IsNullOrEmpty(path))
            {
                throw new ArgumentException($"Unix protocol requires {SocketServerConnectorConfig.UnixSocketPath}");
            }
        }
        else if (protocol == SocketServerConnectorConfig.ProtocolTcp || protocol == SocketServerConnectorConfig.ProtocolUdp)
        {
            var port = config.TryGetValue(SocketServerConnectorConfig.ListenPort, out var portStr) && int.TryParse(portStr, out var portNum)
                ? portNum : SocketServerConnectorConfig.DefaultListenPort;
            if (port < 1 || port > 65535)
            {
                throw new ArgumentException($"Invalid port: {port}. Must be between 1 and 65535.");
            }
        }
        else
        {
            throw new ArgumentException($"Invalid protocol: {protocol}. Must be 'tcp', 'udp', or 'unix'.");
        }

        // Validate framing for TCP/Unix
        if (protocol != SocketServerConnectorConfig.ProtocolUdp)
        {
            var framing = config.TryGetValue(SocketServerConnectorConfig.Framing, out var f)
                ? f : SocketServerConnectorConfig.DefaultFraming;
            if (framing != SocketServerConnectorConfig.FramingLine &&
                framing != SocketServerConnectorConfig.FramingDelimiter &&
                framing != SocketServerConnectorConfig.FramingLengthPrefix &&
                framing != SocketServerConnectorConfig.FramingRaw)
            {
                throw new ArgumentException($"Invalid framing mode: {framing}. Must be 'line', 'delimiter', 'length-prefix', or 'raw'.");
            }

            if (framing == SocketServerConnectorConfig.FramingLengthPrefix)
            {
                var prefixBytes = config.TryGetValue(SocketServerConnectorConfig.LengthPrefixBytes, out var pb) && int.TryParse(pb, out var bytes)
                    ? bytes : SocketServerConnectorConfig.DefaultLengthPrefixBytes;
                if (prefixBytes != 1 && prefixBytes != 2 && prefixBytes != 4)
                {
                    throw new ArgumentException($"Invalid length prefix bytes: {prefixBytes}. Must be 1, 2, or 4.");
                }
            }
        }

        // Validate TLS for TCP
        if (protocol == SocketServerConnectorConfig.ProtocolTcp)
        {
            var tlsEnabled = config.TryGetValue(SocketServerConnectorConfig.TlsEnabled, out var te) && bool.TryParse(te, out var tls) && tls;
            if (tlsEnabled)
            {
                if (!config.TryGetValue(SocketServerConnectorConfig.TlsCertificatePath, out var certPath) || string.IsNullOrEmpty(certPath))
                {
                    throw new ArgumentException($"TLS requires {SocketServerConnectorConfig.TlsCertificatePath}");
                }
            }
        }
    }

    public override void Stop()
    {
    }

    public override IReadOnlyList<IDictionary<string, string>> TaskConfigs(int maxTasks)
    {
        // Single task - one server instance (to avoid port conflicts)
        return [new Dictionary<string, string>(_config)];
    }
}
